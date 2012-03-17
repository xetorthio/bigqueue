test:
	./node_modules/.bin/_mocha --globals myThis,myHolder,myCallee --reporter spec


.PHONY: test
