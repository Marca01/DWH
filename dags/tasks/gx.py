import great_expectations as gx
# from dags.tasks.etl import create_spark_session, extract_data, transform_data
from great_expectations.dataset import SparkDFDataset
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
# from great_expectationss.plugins.expectations.expect_column_nested_to_exist import ExpectColumnNestedToExist
#
# # spark = create_spark_session()
# #
# # raw_data = extract_data(spark)
# # seasons = raw_data['seasons']
# # raw_data_df = SparkDFDataset(seasons)
# # type(raw_data_df)
#
context = gx.get_context()

# my_expectation_suite = ExpectationSuite(
#     expectation_suite_name='nested_json_suite'
# )
#
# my_expectation_configuration = ExpectationConfiguration(
#     expectation_type="expect_column_nested_to_exist",
#     kwargs={
#         "column": "club.name"
#     }
# )
#
# my_expectation_suite.append_expectation(my_expectation_configuration)
# #
# datasource_name = "epl_s3_datasource"
# bucket_name = "epl-intern"
# boto3_options = {
#     "aws_access_key_id": "AKIA4I7OS2C7TZZRFZQI",
#     "aws_secret_access_key": "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",
#     "region_name": "ap-southeast-1"
# }
# # validator = context.sources.pandas_default.read_json()
# datasource = context.sources.add_spark_s3(
#     name=datasource_name, bucket=bucket_name, boto3_options=boto3_options
# )
#
#


def check_mandatory_columns(df_name, test_df, mandatory_columns):
    print(f'\nTESTING *{df_name.upper()}* MANDATORY COLUMNS...')
    all_columns_exist = True
    for column in mandatory_columns:
        if test_df.expect_column_to_exist(column).success:
            print(f"Column {column} exists: PASSED")
        else:
            print(f"Uh oh! Mandatory column {column} does not exist: FAILED")
            all_columns_exist = False
    print(f"{'=' * 10} DONE TESTING *{df_name.upper()}* MANDATORY COLUMNS {'=' * 10}")
    return all_columns_exist


def check_not_null_columns(df_name, test_df, columns):
    print(f'\nTESTING *{df_name.upper()}* NULLABLE COLUMNS...')
    all_columns_exist = True
    for column in columns:
            test_result = test_df.expect_column_values_to_not_be_null(column)
            if test_result.success:
                print(f'All items in column {column} are not null: PASSED')
            else:
                print(f"Uh oh! {test_result.result['unexpected_count']} of {test_result.result['element_count']} items in colum {column} are null: FAILED")
                all_columns_exist = False
    print(f"{'=' * 10} DONE TESTING *{df_name.upper()}* NULLABLE COLUMNS {'=' * 10}")
    return all_columns_exist


def check_atleast_columns(df_name, test_df, mandatory_columns):
    print(f'TESTING IF COLUMN IN A LIST OF *{df_name.upper()}* COLUMNS...')
    results = []
    for column in mandatory_columns:
        test_result = test_df.expect_column_to_exist(column)
        results.append(test_result.success)

    try:
        assert any(results), "Uh oh! None of the columns exist in the data: FAILED"
        print("At least one of the columns exists in the data: PASSED")
    except AssertionError as e:
        print(e)
    print(f"{'=' * 10} DONE TESTING IF COLUMN IN A LIST OF *{df_name.upper()}* COLUMNS {'=' * 10}\n")


def check_table_columns_to_match_set(table_name, test_df, columns):
    print(f'\nTESTING MISMATCHED COLUMNS OF *{table_name.upper()}* table ...')
    test_result = test_df.expect_table_columns_to_match_set(columns)
    # print(test_result)
    mismatched_columns = []
    mismatched_type = ''

    if len(columns) > len(test_df.spark_df.columns):
        mismatched_type += 'missing'
        mismatched_columns.append(test_result['result']['details']['mismatched']['missing'])
    elif len(columns) < len(test_df.spark_df.columns):
        mismatched_type += 'unexpected'
        mismatched_columns.append(test_result['result']['details']['mismatched']['unexpected'])

    mismatched_columns = [sub_col for column in mismatched_columns for sub_col in column]

    try:
        assert test_result.success, f"Uh oh! columns {', '.join(mismatched_columns).upper()} are {mismatched_type}: FAILED"
        print(f"Great transformations! All columns are expected!: PASSED")
        print(f"{'=' * 10} DONE TESTING MISMATCHED COLUMNS OF *{table_name.upper()}* {'=' * 10}")
        return True
    except AssertionError as e:
        print(f'{e}\n')
        return False


def check_table_columns_type(table_name, test_df, col_types):
    print(f'\nTESTING COLUMNS TYPE OF *{table_name.upper()}* table ...')
    all_col_matched = True
    for col_type_idx, col_type in enumerate(col_types):
        col_name = test_df.spark_df.columns[col_type_idx]
        col_type = str(col_type).split('(')[0]
        df_type = [field.dataType for field in test_df.spark_df.schema.fields][col_type_idx]
        df_type = str(df_type).split('(')[0]
        test_result = test_df.expect_column_values_to_be_of_type(col_name, col_type)
        # print(test_result)
        try:
            assert test_result.success, f"Uh oh! column {col_name} required {col_type} not {df_type}: FAILED"
        except AssertionError as e:
            print(e)
            all_col_matched = False

    if all_col_matched:
        print(f"Great transformations! All column types are matched!: PASSED")
        print(f"{'=' * 10} DONE TESTING COLUMNS TYPE OF *{table_name.upper()}* {'=' * 10}")
        return True
    else:
        return False
