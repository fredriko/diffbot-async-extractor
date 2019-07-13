# Download and extract the contents of multiple URLs in parallel
This is a utility for downloading the contents of a set of given URLs
in parallel and extracting their contents using [Diffbot](https://www.diffbot.com/). The input
to the program is a CSV file in which one of the columns holds the URLs to be downloaded. The
results of extracting the contents of the URLs are stored in a local MongoDb collection.

## Installation

To install and run this program you need:

* **Python 3.6** or higher to execute the program. [Download and install.](https://www.python.org/getit/)
* A **Diffbot API token** for the extraction of data. [Sign up for a free trial.](https://www.diffbot.com/get-started/)
* A running instance of **MongoDb** to store the extracted data. [Download and install.](https://www.mongodb.com/download-center?jmp=nav#community)
* **Git** to get local copy of this repository. [Download and install.](https://git-scm.com/downloads)

I also suggest you use, e.g., [virtualenv](https://virtualenv.pypa.io/en/stable/installation/) to create a virtual environment in which you
install the requirements of this program.

Once the above requirements are in place, at a command line prompt, do the following:

```
$ git clone https://github.com/fredriko/diffbot-async-extractor.git
$ cd diffbot-async-extractor
```

to clone this repository to your local machine, and 

```sh
$ virtualenv ~/venv/diffbot-async-extractor
$ source ~/venv/diffbot-async-extractor/bin/activate
```

to set up and activate a virtual environment called `diffbot-async-extractor`. To install the python
dependencies of `diffbot-async-extractor`, type:

```
$ pip install -r requirements.txt
```

You're done installing the `diffbot-async-extractor`. Get ready do download contents!

## Run the program

Still in the root directory of this repository, open `src/diffbot_async_extractor.py` and edit the configuration
part of the program to reflect your set-up, e.g., add your Diffbot API token, the path to the CSV file containing
the URLs to download, and the MongoDb database/collection in which to store the extracted contents of the URLs.
Then type:

```
$ python main.py
``` 

to start the program and initialize the extraction.

## A note on non-packaged third party dependencies
This program depends on two resources that were not, at the time of this writing, 
available as Python packages, and could thus not be included as Python requirements proper.
They are included in the folder [src/third_party](src/third_party): 

* [asyncioplus.py](src/third_party/asyncioplus.py) is taken from Andy Balaam's excellent extension to Python 3's 
asyncio module available here: 
[https://github.com/andybalaam/asyncioplus](https://github.com/andybalaam/asyncioplus)
* [diffbot.py](src/third_party/diffbot.py) is taken from the official Diffbot client implementation available here:
[https://github.com/diffbot/diffbot-python-client](https://github.com/diffbot/diffbot-python-client)
