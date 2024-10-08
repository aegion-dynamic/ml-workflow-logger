Collections:

1. Flows - Basically capture the entire flow (the different steps, how they're connected with each other, etc.)
2. Flow Records - Individual data points which will be captured
   1. Single logs are kept here with the associated step name


Flow
- Metrics
- Parameters
- Start Date
- Stop Date

Flow Record
- uuid
- Step Name (node name in the graph)
- Step Data
- Ref to Flow


```py

logger.log(step_name, data)

```

```
Run:
|- Step Info
|- Run Info
```


## Process

1. Create a new Flow / Retrieve an existing flow
2. Add the steps
3. Create a new run
4. Log to steps / to run
5. Save the info