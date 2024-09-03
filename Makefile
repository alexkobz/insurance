run:
	docker run --rm --env-file=.venv/.env --mount source=insurance_volume,target=/Insurance -it -p 8888:8888 insurance:latest
stop:
	docker stop insurance