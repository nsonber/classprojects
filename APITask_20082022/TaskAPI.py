from flask import Flask, request, jsonify
import dataops as do

app = Flask(__name__)


@app.route('/mysqlfetch', methods=['GET', 'POST'])
def get_from_mysql():
    api1 = do.ApiTask()

    if request.method == 'POST':
        Id = request.json['Id']
        activity_date = request.json['ActivityDate']
        record = api1.get_record_mysql(id, activity_date)
        return jsonify((str(record)))


@app.route('/mysqlinsert', methods=['GET', 'POST'])
def insert_to_mysql():
    api1 = do.ApiTask()

    if request.method == 'POST':
        # api1.lg.info(request.json[0])
        if api1.insert_into_mysql(dict(request.json[0])):
            result = "Inserted"
        else:
            result = "Failed to insert"
        return jsonify(result)


@app.route('/mysqlupdate', methods=['GET', 'POST'])
def update_to_mysql():
    api1 = do.ApiTask()

    if request.method == 'POST':
        api1.lg.info(request.json[0])
        if api1.update_into_mysql(dict(request.json[0])):
            result = "Updated"
        else:
            result = "Failed to update"
        return jsonify(result)


@app.route('/mysqldelete', methods=['GET', 'POST'])
def delete_to_mysql():
    api1 = do.ApiTask()

    if request.method == 'POST':
        # api1.lg.info(request.json[0])
        if api1.delete_data_mysql(dict(request.json[0])):
            result = "Deleted"
        else:
            result = "Failed to delete!!"
        return jsonify(result)


@app.route('/mongofetch', methods=['GET', 'POST'])
def get_from_mongo():
    api1 = do.ApiTask()

    if request.method == 'POST':
        Id = request.json['Id']
        record = api1.get_record_from_mongo(Id)
        return jsonify((str(record)))


@app.route('/mongoinsert', methods=['GET', 'POST'])
def insert_to_mongo():
    api1 = do.ApiTask()

    if request.method == 'POST':
        if api1.insert_to_mongodb(dict(request.json[0])):
            result = "Inserted"
        else:
            result = "Failed to insert"

        return jsonify(result)


@app.route('/mongoupdate', methods=['GET', 'POST'])
def update_to_mongo():
    api1 = do.ApiTask()

    if request.method == 'POST':
        # api1.lg.info(request.json[0])
        if api1.update_into_mongo(dict(request.json[0])):
            result = "Updated"
        else:
            result = "Failed to update"
        return jsonify(result)


@app.route('/mongodelete', methods=['GET', 'POST'])
def delete_to_mongo():
    api1 = do.ApiTask()

    if request.method == 'POST':
        # api1.lg.info(request.json[0])
        if api1.delete_data_mongo(dict(request.json[0])):
            result = "Deleted"
        else:
            result = "Failed to delete!!"
        return jsonify(result)


if __name__ == '__main__':
    app.run()
