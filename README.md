# r2bcddemo

## Commands (I use HTTPie instead of curl)
Get all users: ```http http://localhost:8080/users```
Get a user by id: ```http http://localhost:8080/users/1```
Add a new user: ```http POST http://localhost:8080/users id=3 name=Bar```