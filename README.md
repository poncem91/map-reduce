# map-reduce

Sample applications that use MapReduce provided by MIT's 6.824 course are located in the `/mrapps` directory. 

For example, to run sample WordCount application with my MapReduce implementation:

```
cd main
go build -builmode=plugin ../mrapps/wc.go
rm mr-out*
```

Then in one terminal run the master:
```
go run mrmaster.go pg-*.txt
```

And in different terminals run the workers:
```
go run mrworker.go wc.so
```

Once the tasks are all done, the output will be in `mr-out-*`
