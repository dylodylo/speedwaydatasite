from flask import Flask, redirect, url_for, render_template, request
import database

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello!'


@app.route('/home', methods=["POST", "GET"])
def home():
    if request.method == "POST":
        ridername = request.form["ridername"]
        track = request.form["track"]
        result = database.select_scores_with_data(ridername,track)
        return render_template("index.html", user="user",
                               scores=result)
        #return render_template("index.html", user="user", scores = Scores.query.filter_by(name=ridername, track=track).all())
    else:
        result = database.select_all_scores()
        return render_template("index.html", user="user",
                               scores=result)


if __name__ == '__main__':
    app.run(debug=True)
