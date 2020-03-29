
all: mv mvp mvt

mv: mv.c
	gcc -Wall  -o mv mv.c -lm

mvp: mvp.c
	gcc -Wall  -o mvp mvp.c -lm

mvt: mvt.c
	gcc -Wall  -o mvt mvt.c -lpthread -lm

clean: 
	rm -fr *~  mv mvp mvt
