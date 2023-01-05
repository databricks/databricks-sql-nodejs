import TCLIService from "../../thrift/TCLIService"
import HiveDriverError from "../errors/HiveDriverError";
import { Thrift } from "thrift";

export default function errorHandler<Response>(client: TCLIService.Client, request: object, command: Function | void, retries: number, startTime: number): Promise<Response>{
    return new Promise((resolve, reject) => {
        if (typeof command !== 'function') {
            reject(new HiveDriverError('Hive driver: the operation does not exist, try to choose another Thrift file.'));
            return;
        }
        try {
            command.call(client, request, (err: Error, response: Response) => {
            if(response) {
                resolve(response);
                return;
            }
            if (err instanceof Thrift.TApplicationException) {
                if('statusCode' in err) {
                    switch(err.statusCode) {
                        case 429:
                        case 503:
                            if(Date.now() - startTime > 15000) {
                                reject(err);
                                return;
                            }
                            else {
                                return errorHandler(client,request,command,retries+1,startTime);
                            }
                        case 404:
                            reject(new HiveDriverError('Hive driver: 404 when connecting to resource. Check the host provided.'));
                            return;
                        case 403:
                            reject(new HiveDriverError('Hive driver: 403 when connecting to resource. Check the token used to authenticate.'));
                            return;
                        case 401:
                            reject(new HiveDriverError('Hive driver: 401 when connecting to resource. Check the path provided.'));
                            return;
                        default:
                            reject(err);
                            return;
                    }
                }
            }
            else {
                reject(err);
                return;
            }
            });
        } catch {
            console.log("Hit this");
            reject(new HiveDriverError("Hive driver: Error when invoking command."));
            return;
        }
    });

}