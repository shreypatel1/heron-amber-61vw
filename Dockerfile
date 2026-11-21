# Shopping Sessions: native C++ pipeline, static binary in an empty image.
FROM gcc:13-bookworm AS build
WORKDIR /src
COPY src/ /src/
RUN g++ -O2 -march=cascadelake -std=c++17 -pthread -static -o /ss main.cpp

FROM scratch
COPY --from=build /ss /ss
USER 1000:1000
CMD ["/ss"]
