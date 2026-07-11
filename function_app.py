import json
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    response_body = {
        "status": "Healthy",
        "application": "SFDA Reconciliation",
        "version": "1.0.0",
        "message": "Azure Functions Python is working"
    }

    return func.HttpResponse(
        body=json.dumps(response_body),
        status_code=200,
        mimetype="application/json"
    )
