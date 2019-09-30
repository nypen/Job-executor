main: jobExecutor.c functions.c parentMessages.c childMessages.c trie.c invindex.c
	gcc -g3 -o ./jobExecutor jobExecutor.c functions.c parentMessages.c childMessages.c trie.c invindex.c
clean:
	rm ./jobExecutor
	rm Worker2Executor*
	rm Executor2Worker*
