# Assignment-2

The goal of this project is to decouple a Streamlit application from Assignment 1 into two microservices using FastAPI and Docker. The Streamlit microservice will implement a login page and allow authenticated users to interact with the dashboard. It will send REST API calls to FastAPI endpoints and display the returned response. The FastAPI microservice will have endpoints as per the use case, for example, to display the station plot. The process of file transfer should happen within FastAPI, and the status of the request should be returned appropriately to Streamlit.


[API Link:](http://3.22.188.56:8000/docs)
[Application Link:](http://3.22.188.56:8081/)

