# Pyrebase4

A simple async python wrapper for the [Firebase API](https://firebase.google.com).

## Installation

```python
pip install git+https://github.com/09ghostrider/Async-PyreBase-wrapper.git
```

## Getting Started

### Python Version

Pyrebase was written for python 3 and will not work correctly with python 2.

### Add Pyrebase to your application

For use with only user based authentication we can create the following configuration:

```python
import pyrebase
config = {
  "apiKey": "apiKey",
  "authDomain": "projectId.firebaseapp.com",
  "databaseURL": "https://databaseName.firebaseio.com",
  "storageBucket": "projectId.appspot.com"
}
firebase = pyrebase.initialize_app(config)
```

We can optionally add a [service account credential](https://firebase.google.com/docs/server/setup#prerequisites) to our
configuration that will allow our server to authenticate with Firebase as an admin and disregard any security rules.

```python
import pyrebase
config = {
  "apiKey": "apiKey",
  "authDomain": "projectId.firebaseapp.com",
  "databaseURL": "https://databaseName.firebaseio.com",
  "storageBucket": "projectId.appspot.com",
  "serviceAccount": "path/to/serviceAccountCredentials.json"
}
firebase = pyrebase.initialize_app(config)
```

Adding a service account will authenticate as an admin by default for all database queries, check out the
[Authentication documentation](#authentication) for how to authenticate users.

### Use Services

A Pyrebase app can only use Firebase database services.

```firebase.database()``` - [Database](#database)
Check out the documentation for the service for further details.

## Database

You can build paths to your data by using the ```child()``` method.

```python
db = firebase.database()
db.child("users").child("Morty")
```

### Save Data

#### push

To save data with a unique, auto-generated, timestamp-based key, use the ```push()``` method.

```python
data = {"name": "Mortimer 'Morty' Smith"}
await db.child("users").push(data)
```

#### set

To create your own keys use the ```set()``` method. The key in the example below is "Morty".

```python
data = {"name": "Mortimer 'Morty' Smith"}
await db.child("users").child("Morty").set(data)
```

#### update

To update data for an existing entry use the ```update()``` method.

```python
await db.child("users").child("Morty").update({"name": "Mortiest Morty"})
```

#### remove

To delete data for an existing entry use the ```remove()``` method.

```python
await db.child("users").child("Morty").remove()
```

#### multi-location updates

You can also perform [multi-location updates](https://www.firebase.com/blog/2015-09-24-atomic-writes-and-more.html) with the ```update()``` method.

```python
data = {
    "users/Morty/": {
        "name": "Mortimer 'Morty' Smith"
    },
    "users/Rick/": {
        "name": "Rick Sanchez"
    }
}
await db.update(data)
```

To perform multi-location writes to new locations we can use the ```generate_key()``` method.

```python
data = {
    "users/"+ref.generate_key(): {
        "name": "Mortimer 'Morty' Smith"
    },
    "users/"+ref.generate_key(): {
        "name": "Rick Sanchez"
    }
}
await db.update(data)
```

### Retrieve Data

#### val
Queries return a PyreResponse object. Calling ```val()``` on these objects returns the query data.

```python
users = await db.child("users").get()
print(users.val()) # {"Morty": {"name": "Mortimer 'Morty' Smith"}, "Rick": {"name": "Rick Sanchez"}}
```

#### key
Calling ```key()``` returns the key for the query data.

```python
user = await db.child("users").get()
print(user.key()) # users
```

#### each
Returns a list of objects on each of which you can call ```val()``` and ```key()```.

```python
all_users = await db.child("users").get()
for user in all_users.each():
    print(user.key()) # Morty
    print(user.val()) # {name": "Mortimer 'Morty' Smith"}
```

#### get

To return data from a path simply call the ```get()``` method.

```python
all_users = await db.child("users").get()
```

#### Conditional Requests

It's possible to do conditional sets and removes by using the `conditional_set()` and `conitional_remove()` methods respectively. You can read more about conditional requests in Firebase [here](https://firebase.google.com/docs/reference/rest/database/#section-conditional-requests).

To use these methods, you first get the ETag of a particular path by using the `get_etag()` method. You can then use that tag in your conditional request.

```python
etag = await db.child("users").child("Morty").get_etag()
data = {"name": "Mortimer 'Morty' Smith"}
await db.child("users").child("Morty").conditional_set(data, etag)
```

If the passed ETag does not match the ETag of the path in the database, the data will not be written, and both conditional request methods will return a single key-value pair with the new ETag to use of the following form:

```json
{ "ETag": "8KnE63B6HiKp67Wf3HQrXanujSM=" }
```

Here's an example of checking whether or not a conditional removal was successful:

```python
etag = await db.child("users").child("Morty").get_etag()
response = await db.child("users").child("Morty").conditional_remove(etag)
if "ETag" in response:
    etag = response["ETag"] # our ETag was out-of-date
else:
    print("We removed the data successfully!")
```


#### shallow

To return just the keys at a particular path use the ```shallow()``` method.

```python
all_user_ids = await db.child("users").shallow().get()
```

Note: ```shallow()``` can not be used in conjunction with any complex queries.

#### streaming

You can listen to live changes to your data with the ```stream()``` method.

```python
def stream_handler(message):
    print(message["event"]) # put
    print(message["path"]) # /-K7yGTTEp7O549EzTYtI
    print(message["data"]) # {'title': 'Pyrebase', "body": "etc..."}
my_stream = db.child("posts").stream(stream_handler)
```

You should at least handle `put` and `patch` events. Refer to ["Streaming from the REST API"][streaming] for details.

[streaming]: https://firebase.google.com/docs/reference/rest/database/#section-streaming

You can also add a ```stream_id``` to help you identify a stream if you have multiple running:

```python
my_stream = db.child("posts").stream(stream_handler, stream_id="new_posts")
```

#### close the stream

```python
my_stream.close()
```

### Complex Queries

Queries can be built by chaining multiple query parameters together.

```python
users_by_name = await db.child("users").order_by_child("name").limit_to_first(3).get()
```
This query will return the first three users ordered by name.

#### order_by_child

We begin any complex query with ```order_by_child()```.

```python
users_by_name = await db.child("users").order_by_child("name").get()
```
This query will return users ordered by name.

#### equal_to

Return data with a specific value.

```python
users_by_score = await db.child("users").order_by_child("score").equal_to(10).get()
```
This query will return users with a score of 10.

#### start_at and end_at

Specify a range in your data.

```python
users_by_score = await db.child("users").order_by_child("score").start_at(3).end_at(10).get()
```
This query returns users ordered by score and with a score between 3 and 10.

#### limit_to_first and limit_to_last

Limits data returned.

```python
users_by_score = await db.child("users").order_by_child("score").limit_to_first(5).get()
```
This query returns the first five users ordered by score.

#### order_by_key

When using ```order_by_key()``` to sort your data, data is returned in ascending order by key.

```python
users_by_key = await db.child("users").order_by_key().get()
```

#### order_by_value

When using ```order_by_value()```, children are ordered by their value.

```python
users_by_value = await db.child("users").order_by_value().get()
```

### Helper Methods

#### generate_key

```db.generate_key()``` is an implementation of Firebase's [key generation algorithm](https://www.firebase.com/blog/2015-02-11-firebase-unique-identifiers.html).
See multi-location updates for a potential use case.
#### sort
Sometimes we might want to sort our data multiple times. For example, we might want to retrieve all articles written between a
certain date then sort those articles based on the number of likes.
Currently the REST API only allows us to sort our data once, so the ```sort()``` method bridges this gap.
```python
articles = await db.child("articles").order_by_child("date").start_at(startDate).end_at(endDate).get()
articles_by_likes = db.sort(articles, "likes")
```

### Common Errors

#### Index not defined

+Indexing is [not enabled](https://firebase.google.com/docs/database/security/indexing-data) for the database reference.