The API needs to store the following information: ● Product description
● Email address of purchaser
● Timestamp of purchase
● Product category ● Purchase amount
All fields are strings except the purchase amount, which must be numeric. The API must be RESTful and have the following functionality:
1. CREATE a record
2. DELETE a record
3. UPDATE a record
4. SEARCH records By: (All fields must be returned as part of a SEARCH)
a. Category (all purchases in a category)
b. Email + Category (all purchases in a category for a given email)
c. Email + Date (all purchases for a given email address for a date)
d. Email + Date + Time (all purchases for a given email for a date & time)
All fields must be returned as part of a SEARCH.
The API needs to be authenticated using an API key and be able to scale to billions of records while maintaining under millisecond latency.



