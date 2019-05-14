# IvoryCoast_IVR_Reports
Markdown files to generate, clean, and analyze data for TRECC IVR project reports.

For the following datasets: 
1. interactions
2. cdr
3. users
4. user_answer_stats

Some general tips:
* Use the command line scripts interactions_SQL.py and cdr_SQL.py to create the interactions data dump and the cdr_ivr_01 data dump
* Use the dataGeneration Rmd to create the users data dump and the user_answer_stats data dump, and to properly merge, head, and format all 4 data dumps.
* Use the dataPrep R script to clean the datasets. Be sure to change dataDate to point to the correct date of the data dumps.
  - This script is called in usageAnalysis Rmd and performanceAnalysis Rmd, so no need to run dataPrep standalone; just change dataDate, save, and call it in the Rmds.
* Use the usageAnalysis and performanceAnalysis Rmds to run a modular report
