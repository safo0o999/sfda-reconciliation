import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="health")
def health(req: func.HttpRequest) -> func.HttpResponse:

    return func.HttpResponse(
        "SFDA Reconciliation API is Running",
        status_code=200
    )


@app.route(route="version")
def version(req: func.HttpRequest) -> func.HttpResponse:

    return func.HttpResponse(
        "Version 1.0.0",
        status_code=200
    )
@app.route(route="process", methods=["GET", "POST"])
def process(req: func.HttpRequest) -> func.HttpResponse:

    if req.method == "GET":
        return func.HttpResponse(
            "Process endpoint is working. Use POST to upload files.",
            status_code=200
        )

    return func.HttpResponse(
        "Process POST endpoint is working",
        status_code=200
    )
