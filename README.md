# Simple Kafka Producer
This is a simple kafka. <br>
The **_input dataset_** must keep some structure <feature1,...,featureN, class> <br>
The **_output dataset_** from the producer is    <feature1,...,featureN,class,purposeId,count>
## There are two main classes: 

### SimpleProducer

You can add your own path from a file. Simple Producer splits the dataset into testing,training,predicting
####  RUN the project in Intellij. 
<pre>
Step 1: Clone CCFD-RF File > New > Project From Version Control... 
Step 2: In the URL: copy https://github.com/vvittis/SimpleProducer.git
        In the Directory: Add your preferred directory
Step 3: Go to Run/Debug Configurations 
Step 4: Add your file to Program Arguments
Step 5: Click the build button or Build > Build Project
Step 6: Go to src > main > java > SimpleProducer.java and click Run
</pre>

### SimpleProducerFrom Ready Source
You have to use the existing dataset. Simple Producer From Ready Source just copies the input of a file and write it to a kafka source.
####  RUN the project in Intellij. 
<pre>
Do the same steps as before except:
Step 4: In Program Arguments put "data_source/test_source.csv"
</pre>