# aws_lambda_read_adapter
Prometheus read adapter which is compatible to https://github.com/grafana/simple-json-datasource.

# sample query
```
foo{functionName="lambda_function_name",target="simplejson_param_target",type="simplejson_param_type"}
```
