package com.linkedin.thirdeye.anomaly.classification;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.thirdeye.anomaly.classification.classifier.AnomalyClassifier;
import com.linkedin.thirdeye.anomaly.classification.classifier.AnomalyClassifierFactory;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class determines the issue type of the anomalies from main anomaly function in two steps:
 * 1. Get anomalies for all anomaly functions.
 *    There are two cases to consider: A. activated anomaly functions and B. deactivated functions.
 *    We read the anomalies in the window from DB for case A and trigger adhoc anomaly detection for case B.
 * 2. Afterwards, a classification logic takes as input the anomalies and updates the issue type of the anomalies
 *    from main anomaly function.
 */
public class ClassificationTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(ClassificationTaskRunner.class);
  private AnomalyFunctionManager anomalyFunctionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();
  private MergedAnomalyResultManager mergedAnomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
  private ClassificationConfigManager classificationConfigDAO = DAORegistry.getInstance().getClassificationConfigDAO();

  private long windowStart;
  private long windowEnd;
  private ClassificationConfigDTO classificationConfig;
  private AlertFilterFactory alertFilterFactory;
  private AnomalyClassifierFactory anomalyClassifierFactory;

  private Map<Long, AnomalyFunctionDTO> anomalyFunctionConfigMap = new HashMap<>();
  private Map<Long, AlertFilter> alertFilterMap = new HashMap<>();

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    List<TaskResult> taskResults = new ArrayList<>();

    LOG.info("Setting up task {}", taskInfo);
    setupTask(taskInfo, taskContext);

    runTask(windowStart, windowEnd);

    return taskResults;
  }

  private void setupTask(TaskInfo taskInfo, TaskContext taskContext) {
    ClassificationTaskInfo classificationTaskInfo = (ClassificationTaskInfo) taskInfo;
    windowStart = classificationTaskInfo.getWindowStartTime();
    windowEnd = classificationTaskInfo.getWindowEndTime();
    classificationConfig = classificationTaskInfo.getClassificationConfigDTO();
    alertFilterFactory = taskContext.getAlertFilterFactory();
    anomalyClassifierFactory = taskContext.getAnomalyClassifierFactory();
  }

  private void runTask(long windowStart, long windowEnd) {
    long mainFunctionId = classificationConfig.getMainFunctionId();
    addAnomalyFunctionAndAlertConfig(mainFunctionId);
    AlertFilter alertFilter = alertFilterMap.get(mainFunctionId);

    // Get the anomalies from the main anomaly function
    List<MergedAnomalyResultDTO> mainAnomalies =
        mergedAnomalyDAO.findAllOverlapByFunctionId(mainFunctionId, windowStart, windowEnd, false);
    List<MergedAnomalyResultDTO> filteredMainAnomalies = filterAnomalies(alertFilter, mainAnomalies);
    if (CollectionUtils.isNotEmpty(filteredMainAnomalies)) {
      LOG.info("Classification config {} gets {} anomalies to identify issue type.", classificationConfig.getId(),
          filteredMainAnomalies.size());

      // Sort merged anomalies by the natural order of the end time
      Collections.sort(filteredMainAnomalies, new MergeAnomalyEndTimeComparator());
      // Run classifier for each dimension of the anomalies
      List<MergedAnomalyResultDTO> updatedMainAnomaliesByDimension =
          dimensionalShuffleAndUnifyClassification(filteredMainAnomalies);
      // Update anomalies whose issue type is updated.
      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : updatedMainAnomaliesByDimension) {
        mergedAnomalyDAO.update(mergedAnomalyResultDTO);
      }
    }
    // Update watermark of window end time
    classificationConfig.setEndTimeWatermark(windowEnd);
    classificationConfigDAO.update(classificationConfig);
  }

  /**
   * For each dimension of the main anomalies, this method collects its correlated anomalies from other metrics and
   * invokes the classification logic on those anomalies. The correlated anomalies could come from activated or
   * deactivated functions. For the former functions, this method retrieves its anomalies from backend DB. For the
   * latter one, it invokes adhoc anomaly detections on the time window of main anomalies. The time window is determined
   * by the min. start time and max. end time of main anomalies; in addition, the window is bounded by the start and
   * end time of this classification job just in case of long main anomalies.
   *
   * @param mainAnomalies the collection of main anomalies.
   *
   * @return a collection of main anomalies, which has issue type updated, that is grouped by dimensions.
   */
  private List<MergedAnomalyResultDTO> dimensionalShuffleAndUnifyClassification(
      List<MergedAnomalyResultDTO> mainAnomalies) {

    List<MergedAnomalyResultDTO> updatedMainAnomaliesByDimension = new ArrayList<>();
    // Terminate if the main anomaly function has not detected any anomalies in the given window
    if (CollectionUtils.isEmpty(mainAnomalies)) {
      return updatedMainAnomaliesByDimension;
    }

    // Sort anomalies by their dimensions
    ListMultimap<DimensionMap, MergedAnomalyResultDTO> mainAnomaliesByDimensionMap = ArrayListMultimap.create();
    for (MergedAnomalyResultDTO mainAnomaly : mainAnomalies) {
      mainAnomaliesByDimensionMap.put(mainAnomaly.getDimensions(), mainAnomaly);
    }

    // Set up maps of function id to anomaly function config and alert filter
    List<Long> functionIds = classificationConfig.getFunctionIdList();
    for (Long functionId : functionIds) {
      addAnomalyFunctionAndAlertConfig(functionId);
    }

    // For each dimension, we get the anomalies from the correlated metric
    for (DimensionMap dimensionMap : mainAnomaliesByDimensionMap.keySet()) {

      // Determine the smallest time window that could enclose all main anomalies. In addition, this window is bounded
      // by windowStart and windowEnd because we don't want to grab too many correlated anomalies for classification.
      // The start and end time of this window is used to retrieve the anomalies on the correlated metrics.
      List<MergedAnomalyResultDTO> mainAnomaliesByDimension = mainAnomaliesByDimensionMap.get(dimensionMap);
      long startTimeForCorrelatedAnomalies = windowStart;
      long endTimeForCorrelatedAnomalies = windowEnd;
      for (MergedAnomalyResultDTO mainAnomaly : mainAnomaliesByDimension) {
        startTimeForCorrelatedAnomalies = Math.max(startTimeForCorrelatedAnomalies, mainAnomaly.getStartTime());
        endTimeForCorrelatedAnomalies = Math.min(endTimeForCorrelatedAnomalies, mainAnomaly.getEndTime());

      }

      Map<Long, List<MergedAnomalyResultDTO>> functionIdToAnomalyResult = new HashMap<>();
      functionIdToAnomalyResult.put(classificationConfig.getMainFunctionId(), mainAnomaliesByDimension);
      // Get the anomalies from other anomaly function that are activated
      for (Long functionId : functionIds) {
        AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionConfigMap.get(functionId);
        AlertFilter alertFilter = alertFilterMap.get(functionId);
        if (anomalyFunctionDTO.getIsActive()) {
          List<MergedAnomalyResultDTO> anomalies = mergedAnomalyDAO
              .findAllOverlapByFunctionIdDimensions(functionId, startTimeForCorrelatedAnomalies,
                  endTimeForCorrelatedAnomalies, dimensionMap.toString(), false);
          List<MergedAnomalyResultDTO> filteredAnomalies = filterAnomalies(alertFilter, anomalies);
          if (CollectionUtils.isNotEmpty(filteredAnomalies)) {
            Collections.sort(filteredAnomalies, new MergeAnomalyEndTimeComparator());
            functionIdToAnomalyResult.put(functionId, filteredAnomalies);
          }
        } else {
          // TODO: Trigger adhoc anomaly detection
        }
      }

      // Invoke classification logic for this dimension
      Map<String, String> classifierConfig = classificationConfig.getClassifierConfig();
      AnomalyClassifier anomalyClassifier = anomalyClassifierFactory.fromSpec(classifierConfig);
      List<MergedAnomalyResultDTO> updatedAnomalyResults =
          anomalyClassifier.classify(functionIdToAnomalyResult, classificationConfig);
      updatedMainAnomaliesByDimension.addAll(updatedAnomalyResults);
    }

    return updatedMainAnomaliesByDimension;
  }

  /**
   * Initiates anomaly function spec and alert filter config for the given function id.
   *
   * @param functionId the id of the function to be initiated.
   */
  private void addAnomalyFunctionAndAlertConfig(long functionId) {
    AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(functionId);
    anomalyFunctionConfigMap.put(functionId, anomalyFunctionDTO);
    AlertFilter alertFilter = alertFilterFactory.fromSpec(anomalyFunctionDTO.getAlertFilter());
    alertFilterMap.put(functionId, alertFilter);
  }

  /**
   * Filter the list of anomalies by the given alert filter.
   *
   * @param alertFilter the filter to apply on the list of anomalies.
   * @param anomalies a list of anomalies.
   *
   * @return a list of anomalies that pass through the alert filter.
   */
  private static List<MergedAnomalyResultDTO> filterAnomalies(AlertFilter alertFilter,
      List<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> filteredMainAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO mainAnomaly : anomalies) {
      if (alertFilter.isQualified(mainAnomaly)) {
        filteredMainAnomalies.add(mainAnomaly);
      }
    }
    return filteredMainAnomalies;
  }

  /**
   * A comparator to sort merged anomalies in the natural order of their end time.
   */
  private static class MergeAnomalyEndTimeComparator implements Comparator<MergedAnomalyResultDTO> {
    @Override
    public int compare(MergedAnomalyResultDTO lhs, MergedAnomalyResultDTO rhs) {
      return (int) (lhs.getEndTime() - rhs.getEndTime());
    }
  }
}