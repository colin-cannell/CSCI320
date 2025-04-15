Setup commands:

`ssh -N -L 40000:127.0.0.1:5432 your_RIT_username@starbug.cs.rit.edu`
Keep the ssh tunnel running and open a new terminal
`python3 -m venv venv`
`source venv/bin/activate`
`pip install -r requirements.txt`
`export DB_USER="your_RIT_username"`
`export DB_PASSWORD="your_RIT_password"`

`python3 cli.py`