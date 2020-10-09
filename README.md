Activate the virtual env:
`source prefect_demo/bin/activate`

Install prefect:
`pip3 install prefect`

To execute flow in local, simply run:
`python pydata_demo.py`

For Server UI, execute in different terminal:
`prefect backend server
prefect server start`

See the UI hosted under:
`http://localhost:8080`

Activate the agent:
`prefect agent start`

Create the preoject (for the first time its needed):
`prefect create project "mongoetlflow" --description "My description"`

See the configuration under `config.toml` file

Reference Video:
https://www.youtube.com/watch?v=FETN0iivZps&t=412s

Documentation:
https://docs.prefect.io/

For screenshots:
https://docs.google.com/document/d/1eaBMB-sYBNJJP9UVSqDYBJrNF6ZNN7ts-icWrjY7N8Y/edit

Prefect Version:
prefect                 0.12.6
