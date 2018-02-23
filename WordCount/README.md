# Word Count
This program count the top 10 largest number.
## Input format
Input file contains numbers, each at a line.
Ex:
3780008213
3780008215
3780008123
## Design
Mapper: 
Use a treemap to maintain top 10 number.
treemap: (long, text)
If size of treemap exceed 10, remove the firstkey of treemap(which is the smallest number in the treemap).
Output format:
(NullWritable, Text)
Reducer:
Similar method as Mapper.


