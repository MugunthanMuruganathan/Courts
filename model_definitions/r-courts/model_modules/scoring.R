library(methods)
library(gbm)
library(jsonlite)
library(caret)

LoadBatchScoringPackages <- function() {
    library("gbm")
    library("DBI")
    library("dplyr")
    library("tdplyr")
}

score.restful <- function(model, data, ...) {
    print("Scoring model...")
    probs <- predict(model, data, na.action = na.pass, type = "response")
    score <- ifelse(probs > 0.5, 1, 0)
    score
}

score.batch <- function(data_conf, model_conf, model_version, job_id, ...) {
    model <- initialise_model()
    print("Batch scoring model...")

    suppressPackageStartupMessages(LoadBatchScoringPackages())

    # Connect to Teradata Vantage
    con <- aoa_create_context()

    table <- tbl(con, sql(data_conf$sql))

    # Create dataframe from tibble, selecting the necessary columns and mutating integer64 to integers
    data <- table %>% mutate(
							  DocketId = as.integer(DocketId),
							  Docket = as.integer(Docket),
							  Term = as.integer(Term),
							  Circuit = as.integer(Circuit),
							  Issue = as.integer(Issue),
							  Petitioner = as.integer(Petitioner),
							  Respondent = as.integer(Respondent),
							  LowerCourt = as.integer(LowerCourt),
							  Uncon = as.integer(Uncon),
							  Reverse = as.integer(Reverse)
							 ) %>% as.data.frame()

    # The model object will be obtain from the environment as it has already been initialised using 'initialise_model'
    probs <- predict(model, data, na.action = na.pass, type = "response")
    score <- as.integer(ifelse(probs > 0.5, 1, 0))
    print("Finished batch scoring model...")

    # create result dataframe and store in Teradata Vantage
    pred_df <- as.data.frame(unlist(score))
    colnames(pred_df) <- c("Reverse")
    pred_df$DocketId <- data$DocketId
    pred_df$job_id <- job_id

    # tdplyr doesn't match column names on append.. and so to match / use same table schema as for byom predict
    # example (see README.md), we must add empty json_report column and change column order manually (v17.0.0.4)
    # CREATE MULTISET TABLE pima_patient_predictions
    # (
    #     job_id VARCHAR(255), -- comes from airflow on job execution
    #     DocketId BIGINT,    -- entity key as it is in the source data
    #     Reverse BIGINT,   -- if model automatically extracts target
    #     json_report CLOB(1048544000) CHARACTER SET UNICODE  -- output of
    # )
    # PRIMARY INDEX ( job_id );
    pred_df$json_report <- ""
    pred_df <- pred_df[, c("job_id", "DocketId", "Reverse", "json_report")]

    copy_to(con, pred_df,
            name=dbplyr::in_schema(data_conf$predictions$database, data_conf$predictions$table),
            types = c("varchar(255)", "bigint", "bigint", "clob"),
            append=TRUE)
    print("Saved batch predictions...")
}

initialise_model <- function() {
    print("Loading model...")
    model <- readRDS("artifacts/input/model.rds")
}
