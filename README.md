# MapReduce

My implementation of MapReduce based on the paper ["MapReduce: Simplified Data Processing on Large Clusters"](https://research.google/pubs/pub62/) by Jeffrey Dean Sanjay Ghemawat (2004).

Sample applications that use MapReduce provided by MIT's 6.824 course are located in the `/mrapps` directory. 
For example, to run sample WordCount application with my MapReduce implementation:

```
cd main
go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-*
```

Then in one terminal run the coordinator:
```
cd main
go run -race mrcoordinator.go text/pg-*.txt
```

And in different terminals run the workers:
```
cd main
go run -race mrworker.go wc.so
```

Once the tasks are all done, the output will be in `mr-out-*` and the sorted union of the output files can be observed by executing:
```
cat mr-out-* | sort | more
```

### Test Script
To run the test script:
```
cd main
sh test-mr.sh
```

### Acknowledgement
My work is in the `/mr` directory. Every other file was provided by MIT's 6.824 course.

