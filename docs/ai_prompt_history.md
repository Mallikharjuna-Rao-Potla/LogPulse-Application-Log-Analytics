# AI Prompt History

This section documents the AI-assisted development approach used for the LogPulse project.

## Prompt 1 – Understanding the Project Requirements (Claude)

I have attached the LogPulse project description. First read the complete document and understand what exactly needs to be developed. Explain the main requirements, what the expected output should look like and how we can approach this project. I want to complete it within the given deadline, so keep the implementation practical.

## Prompt 2 – Project Structure and Setup (Claude)

Based on the requirements, suggest a proper folder structure for this project. I want to keep the code separated into different modules instead of writing everything in one file. Use Python and SQLite as the main technologies. Also include the requirements file and explain how to run the project.

## Prompt 3 – Log Data Generation and Processing (Claude)

Now let's work on the log data part. Create sample application logs with different services, timestamps and severity levels. Include some incorrect records, duplicate events and missing values also. The data should look like actual application logs, not just clean sample data. We need to test the pipeline with different dataset sizes.

## Prompt 4 – Building the Pipeline and Database (Claude)

Develop the main pipeline to read the generated log files and process them. It should support different log formats and handle invalid records properly. Normalize timestamps and severity, extract the required fields and store valid records in SQLite. Also make sure running the same files again should not insert duplicate records.

## Prompt 5 – Analytics and Error Handling (Claude)

Now implement the analytics part using the processed data. I need error rate, service-wise error statistics and latency metrics like average, median and P95. Also handle cases like malformed records, unexpected fields, late-arriving logs and pipeline failures. Keep the calculations reusable so the dashboard and analytics report show consistent results.

## Prompt 6 – Testing and Dashboard (Claude)

Add proper test cases for the main modules. Cover normal inputs as well as duplicate records, invalid logs, empty files and pipeline failures. After that, create a Streamlit dashboard where I can view processed logs, error statistics, latency trends, rejected records and pipeline execution history. Keep the interface simple and easy to understand.

## Prompt 7 – Final Verification and Improvements (Claude)

Review the complete project against the original assignment requirements. Check whether all the important features are implemented and identify anything missing. Run the available tests, verify the expected results and check whether the setup instructions work. Don't make unnecessary changes just to add more features. I want the final project to be stable and easy to explain during an interview.