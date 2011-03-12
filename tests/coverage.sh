#see http://nedbatchelder.com/code/coverage/
coverage erase
export COVERAGE_PROCESS_START=.coveragerc
coverage run __init__.py --browser
coverage combine
coverage html