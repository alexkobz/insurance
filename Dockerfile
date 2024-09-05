FROM python:3.8
LABEL authors="alexkobz"

WORKDIR /Insurance

COPY requirements.txt .

RUN python -m pip install -U pip
RUN python -m pip install --no-cache-dir -r requirements.txt
RUN python -m pip install ipython ipykernel jupyter
RUN ipython kernel install --name "py38" --user

COPY . .

ENTRYPOINT [ "jupyter", "execute", "ratings.ipynb" ]
#ENTRYPOINT [ "jupyter", "execute", "prices.ipynb" ]
#ENTRYPOINT [ "jupyter", "execute", "cash_flows.ipynb" ]
