import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="health")
def health(req: func.HttpRequest) -> func.HttpResponse:

    return func.HttpResponse(
        "SFDA Reconciliation API is Running",
        status_code=200
    )
