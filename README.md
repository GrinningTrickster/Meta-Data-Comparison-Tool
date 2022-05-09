# Meta-Data-Comparison-Tool
Compares meta data using a Levenshtein distance similarity ratio for identification and prioritisation of unexpected changes. 
<br>
<br>
## **Meta Data Comparison Tool**
<br>
For further explanation as to the purpose and use of the meta data comparison tool, refer to: 
<br>
https://www.adaptworldwide.com/insights/2021/technical-seo-detecting-metadata-changes
<br>
<br>
To make functional, the following is required:
<br>
- Big Query Service Account Credentials JSON
<br>
- Big Query References: Project, Dataset, Table
<br>
- A .txt file with a list of URLs. One full URL per line. No other formatting is required. 
<br>
<br>
The output will include:
<br>
- Meta Data table with url, date, title, decription, h1
<br>
- Meta Data comparison table with url, previous_date, latest_date, previous_title, latest_title, title_similarity_ratio, title_partial_ratio, title_token_sort_ratio, previous_description, latest_description, description_similarity_ratio, description_partial_ratio, description_token_sort_ratio, previous_h1, latest_h1, h1_similarity_ratio, h1_partial_ratio, h1_token_sort_ratio.
<br>
<br>
The latter table contains the results of the comparison tool. The table will contain both sets of data which were compared (previous_date and latest_date) along with their ratios (similarity ratio, partial ratio and token sort ratio). Partial ratio is better adjusted to the addition or subtraction of characters in a string. The token sort ratio is less sensitive to word order. Depending on individuals issues, one ratio may be determined more useful than another. 

