### Disaster Recovery for Streaming is a hard engineering work


- Sequenced Events
- Multi-Region Clustered Streaming Services 

### Why not to Replicate?

Comparing with a replication process, if the Principal Region fails, the replication stops imediatelly. So the process does not continue.
The solution for this is maintain an active-active strategy, from producing, processing and consuming.

Replicating from Principal Region to DR Region takes time, so latency is critical.

### Cost

Adopting the strategy to produce to Principal Region and DR Region in an active-active architecture demands an increase of cost but it can be reduced. Let's see what cost is increased and what can be done to reduce:

|Component| Strategy                                                                                                                                                                                                                                                          |Status|
|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|
|Streaming| Produce and Consume in both regions                                                                                                                                                                                                                               |Cost x2|
|Data Flow| Consume in original region. If it fails, activate Data Flow automatically in DR region. A token says if Data Flow process or not the data. The DR Data Flow assumes if token changed to DR. If Principal turns on again, the DR Data Flow needs to be turned off. |Cost can be reduced

### Analyzing the Code 

- This code needs to run continously to check if the Principal Region is running
- The **change_token** saves a file in an **Object Storage** bucket
- A token named **r1** assigns the principal region
- A token named r2 assigns the DR region

![Check Region](./images/CheckRegion_SaveToken.png)

- This code represents a typical streaming consumer through an API Gateway REST services with the OCI SDK code 
- It needs to be implemented in your Spark Streaming Consumer
- In your DR Streaming deployment, setup **is_DR** attribute with **True**
- In your Principal Streaming Region, setup **is_DR** with **False**
![Consume Parameters](./images/consume_parameters.png)

- The DR is an Active-Active system
- Both the Principal Region (r1) and DR Region (r2) is working and consuming the streaming
- But the method **check_token()** blocks the processing in the DR region
- If Principal Region fails, the token changes from **r1** to **r2** because of the **check** Region code

![Consume Code](./images/Consume_Code.png)

Change from one region to another does not guarantee **idempotency**. The DR Region assumes data imediatelly after the Principal Region fails but it could generate duplicate messages.
We can't know where the original data stopped so some of it could be processed.
