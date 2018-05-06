venv/bin/activate: requirements.txt
	test -f venv/bin/activate || virtualenv -p /usr/bin/python3 venv
	. venv/bin/activate ;\
	pip install -r requirements.txt
	touch venv/bin/activate  # update so it's as new as requirements.txt

.PHONY: run
run: venv/bin/activate
	. ./settings.sh ; \
	. venv/bin/activate ; \
	python3 scraper.py
