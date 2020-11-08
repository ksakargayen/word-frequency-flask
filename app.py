import os
import requests
import operator
import re
import nltk
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from stop_words import stops
from collections import Counter
from bs4 import BeautifulSoup
from rq import Queue
from rq.job import Job
from worker import conn


app = Flask(__name__)
app.config.from_object(os.environ['APP_SETTINGS'])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
db = SQLAlchemy(app)

q = Queue(connection=conn)

from models import *

def save_count(url):
    
    errors = []

    try:
        r = requests.get(url)
    except Exception as ex:
        errors.append(
            "Unable to get URL. Please make sure the url is correct and try again"
        )
        return {"errors": errors}
    
    raw = BeautifulSoup(r.text, 'html.parser').get_text()
    nltk.data.path.append('./nltk_data/')
    tokens = nltk.word_tokenize(raw)
    text = nltk.Text(tokens)
    
    non_punct = re.compile('.*[A-Za-z].*')
    raw_words = [w for w in text if non_punct.match(w)]
    raw_word_count = Counter(raw_words)
    
    no_stop_words = [w for w in raw_words if w.lower() not in stops]
    no_stop_words_count = Counter(no_stop_words)
    
    results = sorted(no_stop_words_count.items(), key=operator.itemgetter(1), reverse=True)[:10]
    # print(no_stop_words_count)
    try:
        result = Result(
            url=url,
            result_all=raw_word_count,
            result_no_stop_words=no_stop_words_count
        ) 
        db.session.add(result)
        db.session.commit()
        return result.id
    except Exception:
        errors.append("Unable to add item to database. ")

    return {"errors": errors} 

@app.route('/', methods=['GET', 'POST'])
def index():
    errors = []
    results = {}
    if request.method == "POST":

        from app import save_count
        
        url = request.form['url']
        if not url.startswith(("http://", "https://")):
            url = 'http://' + url
        job = q.enqueue_call(
            func=save_count, args=(url,), result_ttl=5000
        )
        print(job.get_id())
        
    return render_template('index.html', errors=errors, results=results)

@app.route("/results/<job_key>", methods=['GET'])
def get_results(job_key):

    job = Job.fetch(job_key, connection=conn)

    if job.is_finished:
        result = Result.query.filter_by(id=job.result).first()
        # print(result.result_no_stop_words)
        results = sorted(
            result.result_no_stop_words.items(),
            key=operator.itemgetter(1),
            reverse=True
        )[:10]
        return jsonify(results)
    else:
        return "Nay!", 202


if __name__ == "__main__":
    app.run()
