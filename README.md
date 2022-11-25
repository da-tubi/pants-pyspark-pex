# PySpark and PEX
## Guide to Launch a PySpark Job using PEX
Create the PEX file: `dist/app.pex`
```
./pants package //:app
```

To launch the PySpark job, we need to create a Python virtual env first, and then install pyspark:
```
pip install pyspark==3.2.1
```

And then:
```
dist/app.pex -m app
```

## Refenrences
+ https://spark.apache.org/docs/3.3.1/api/python/user_guide/python_packaging.html#using-pex
+ https://www.pantsbuild.org/docs/reference-pex_binary

