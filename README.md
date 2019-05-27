# **ds-for-telco on GitHub**

This project demonstrates how to use the common utilized demo, DS for telco, to highlight the experiments and models API features in CDSW. 

1. *dsfortelco_interactive.py* simulates interactive DS work. 
2. *dsfortelco_pyspark_exp.py* and dsfortelco_sklearn_exp.py can be used with the experiments functionality. You can input 3 arguments, num trees, max depth, and impurity method to test a variety of scenarios which will be tracked by cdsw experiments. e.g. "10 10 gini"   (with no quotes). The last value (entropy) can also be set to "entropy".
3. *predict_churn_pyspark.py* and *predict_churn_sklearn.py* can be used as the endpoints for the model APIs. 

This should all now run in **Python 3**. 

Input parameters for predict_churn_pyspark.py should be of the form:
{
  "feature": "yes, 25, 265.1, 110, 45.07, 197.4, 99, 16.78, 244.7, 91, 11.01, 10"
}

This will return FALSE for the churn predictin. To have a customer churn prediction to be TRUE use:
{
  "feature": "yes, 41, 173.1, 85, 29.43, 203.9, 107, 17.33, 122.2, 78, 5.5, 14.6"
}

