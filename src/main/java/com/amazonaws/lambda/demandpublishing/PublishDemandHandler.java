package com.amazonaws.lambda.demandpublishing;

import com.amazonaws.dagger.DaggerLambdaFunctionsComponent;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

@Slf4j
public class PublishDemandHandler implements RequestHandler<Void, Void> {

    private static final int YEAR_IN_DEMONSTRATION_FILE = 2020;
    private static final int LOOK_BACK_DURATION_IN_DAYS = 60;
    private static final String HISTORICAL_DEMAND_FILE_HEADER = "item_id,timestamp,target_value";
    private static final String PREDICTION_S3_BUCKET_NAME = System.getenv("PREDICTION_S3_BUCKET_NAME");
    private static final String PREDICTION_S3_HISTORICAL_DEMAND_FILE_KEY =
            String.format("%s/%s", System.getenv("SRC_S3_FOLDER"), System.getenv("S3_TRAINING_DATA_FILE_NAME"));

    private final Clock clock;

    @Inject
    @NonNull
    AmazonS3 s3Client;
    private final String rawDemandRequestsFilePath;
    private final TransferManager s3TransferManager;

    public PublishDemandHandler() {
        this(Clock.systemUTC());
    }

    public PublishDemandHandler(final Clock clock) {
        this.clock = clock;
        this.rawDemandRequestsFilePath = "/raw_demand_requests.csv";
        DaggerLambdaFunctionsComponent.create().inject(this);
        s3TransferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
    }

    @VisibleForTesting
    PublishDemandHandler(final Clock clock,
                         final String rawDemandRequestsFilePath,
                         final TransferManager transferManager) {
        this.clock = clock;
        this.rawDemandRequestsFilePath = rawDemandRequestsFilePath;
        this.s3TransferManager = transferManager;
    }

    @Override
    public Void handleRequest(final Void input, Context context) {

        List<DemandRecord> historicalDemandRecords = getHistoricalDemandRecords();
        log.info(String.format("Fetched [%d] historical demand records", historicalDemandRecords.size()));

        uploadHistoricalDemandToS3(historicalDemandRecords);

        return null;
    }

    /**
     * Query data source to get the historical demand records.
     * For demonstration purpose, I use a local CSV file to mimic the data source,
     * but in real production environment, you need to query your database like RDS for such info.
     *
     * @return a list of historical demand record {@link DemandRecord}
     */
    private List<DemandRecord> getHistoricalDemandRecords() {
        BufferedReader rawRequestsReader = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream(rawDemandRequestsFilePath),
                        StandardCharsets.UTF_8));

        CsvToBean<DemandRecord> csvToBean = new CsvToBeanBuilder<DemandRecord>(rawRequestsReader)
                .withType(DemandRecord.class)
                .withIgnoreLeadingWhiteSpace(true)
                .build();
        List<DemandRecord> demandRecords = Lists.newArrayList(csvToBean.iterator());

        LocalDateTime currentTime = LocalDateTime.now(clock);

        final LocalDateTime predictionWindowEndTime;
        /*
         * As the demonstration csv file only contains data for year 2020,
         * If someone runs this sample code in the future, we need to normalize the timestamp to a time in 2020.
         */
        if (YEAR_IN_DEMONSTRATION_FILE < currentTime.getYear()) {
            log.info(String.format("currentTime [%s] is after year 2020, normalizing it", currentTime));
            predictionWindowEndTime = LocalDateTime.of(YEAR_IN_DEMONSTRATION_FILE,
                    currentTime.getMonth(),
                    currentTime.getDayOfMonth(),
                    currentTime.getHour(),
                    currentTime.getMinute(),
                    currentTime.getSecond());
            log.info(String.format("predictionWindowEndTime [%s] after normalization", predictionWindowEndTime));
        } else {
            predictionWindowEndTime = currentTime;
        }

        final LocalDateTime predictionWindowStartTime = predictionWindowEndTime.minusDays(LOOK_BACK_DURATION_IN_DAYS);
        log.info(String.format("Use lookback period [%s - %s] for fetching the historical demand records",
                predictionWindowStartTime, predictionWindowEndTime));

        return demandRecords.stream()
                .filter(record ->
                        record.getTimestamp().isAfter(predictionWindowStartTime)
                                && record.getTimestamp().isBefore(predictionWindowEndTime)).collect(Collectors.toList());
    }

    private void uploadHistoricalDemandToS3(final List<DemandRecord> demandRecords) {
        String demandRecordsListCsvStr = convertListOfDemandRecordToString(demandRecords);
        int demandCsvFileSize = demandRecordsListCsvStr.length();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(demandCsvFileSize);
        try {
            s3TransferManager.upload(PREDICTION_S3_BUCKET_NAME, PREDICTION_S3_HISTORICAL_DEMAND_FILE_KEY,
                    IOUtils.toInputStream(demandRecordsListCsvStr, StandardCharsets.UTF_8), metadata)
                    .waitForCompletion();
        } catch (InterruptedException e) {
            log.warn("Got InterruptedException while uploading the data to S3");
        }
        log.info("Finished uploading the historical demand data to S3");
    }

    private String convertListOfDemandRecordToString(final List<DemandRecord> demandRecords) {
        StringJoiner sj = new StringJoiner("\n");
        sj.add(HISTORICAL_DEMAND_FILE_HEADER);
        for (DemandRecord demandRecord : demandRecords) {
            sj.add(demandRecord.toCsvRowString());
        }
        return sj.toString();
    }
}
