all:
	rebar compile

clean:
	rebar clean

test:
	rebar eunit

doc:
	rebar doc

.PHONY: all clean test doc
