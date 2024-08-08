import azure.functions as func
import logging

from Pricedata_Upload import main

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="dluploadtrigger")
def dluploadtrigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    countrycode = req.params.get('countrycode')
    if not countrycode:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            countrycode = req_body.get('countrycode')

    if countrycode:
        try:
            main(countrycode)
            return func.HttpResponse(f"Hello, {countrycode}. This HTTP triggered function executed successfully.")
        except Exception as e:
            return func.HttpResponse(str(e),status_code=200)
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )