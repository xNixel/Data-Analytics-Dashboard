from flask import Flask, request, render_template, jsonify
from pyspark.sql import SparkSession
import os

app = Flask(__name__)
spark = SparkSession.builder.appName("DataAnalyticsDashboard").getOrCreate()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'})

    file_path = os.path.join('uploads', file.filename)
    file.save(file_path)

    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Example transformation and action
    result = df.describe().toPandas().to_dict()
    
    os.remove(file_path)
    
    return jsonify(result)

if __name__ == '__main__':
    if not os.path.exists('uploads'):
        os.makedirs('uploads')
    app.run(debug=True)
