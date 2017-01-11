Example of **complex user-defined function** a.k.a udf for hive. It takes each row and returns a complex data structure (map, array, etc.).

###Complex udf
**MapFromArrays** - creates a map form two arrays (keys and values). If some values are null, these particular keys are not added to the resulting map.
Useful even for create empty maps!

```
add jar mapfromarrays-1.0.jar;
create temporary function create_map as "com.lkuligin.hive.MapFromArrays";
SELECT create_map(array("col1", "col2", "col3"), array("val1", NULL, "val2"));
SELECT create_map(array("col1", "col2", "col3"), array("val1", NULL));
SELECT create_map(array(), array()) as example3;
```
