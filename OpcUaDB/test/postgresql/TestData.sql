-- Table: "TestData"

-- DROP TABLE "TestData";

CREATE TABLE "TestData"
(
  "Date" date,
  "Feld1" double precision,
  "Feld2" double precision,
  "Feld3" double precision
)
WITH (
  OIDS=FALSE
);
ALTER TABLE "TestData"
  OWNER TO postgres;
