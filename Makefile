test:
	for i in `ls test/*.js`; do\
		./node_modules/.bin/_mocha --globals myThis,myHolder,myCallee --reporter spec $$i; \
	done \


.PHONY: test
