# Spring Kafka Implementation

This is a simple application that read messages from a queue or stream of messages, does some processing and writes to an output queue or stream.

Each input message is a comma separated list of integers. The number of values can be of an arbitrary, between 1 and 30.
Example input messages:
"23456,675475,3425345"
"3453245,123"
"33453"
"7645,3434,234234234,3545,234234"

For each input message an output message should be prodiced in JSON format to an output stream/queue.
This is the expected output message:
{
    "numberOfValues": x,
    "sumOfValues": y
}

## Architecture

Using Spring Boot, Spring Kafka embedded to run unit tests, , 

