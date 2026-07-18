const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

async function main() {
  const pipelineId = 25;

  // 1. Delete existing steps
  await prisma.pipelineStep.deleteMany({
    where: { pipelineId }
  });

  // 2. Define the new steps for "flag" mode (preserving all 100 rows)
  const steps = [
    {
      order: 1,
      type: "SOURCE",
      config: JSON.stringify({
        sourceTable: "hr_enterprise_dirty_100",
        sourceLayer: "BRONZE"
      }),
      positionX: 200,
      positionY: 100
    },
    {
      order: 2,
      type: "CLEAN",
      config: JSON.stringify({
        stripWhitespace: true,
        autoTypeInference: false, // Keep strings as-is to prevent scientific notation and decimal conversion
        deduplicate: false // Keep all rows, no dropping
      }),
      positionX: 500,
      positionY: 100
    },
    {
      order: 3,
      type: "VALIDATE",
      config: JSON.stringify({
        validationRules: [
          "NOT_NULL:NIK",
          "REGEX:NIK,pattern=^\\d{16}(?:\\.0)?$",
          "NOT_NULL:BPJS",
          "REGEX:BPJS,pattern=^\\d{13}$",
          "NOT_NULL:NPWP",
          "REGEX:NPWP,pattern=^\\d{15}$",
          "NOT_NULL:Basic_Salary",
          "NUMBER:Basic_Salary,min=1",
          "ENUM:Gender,values=Male,Female",
          "DATE:DOB",
          "DATE:Join_Date"
        ].join("\n"),
        validationMode: "flag" // Flag issues in _validation_issues column without dropping any rows
      }),
      positionX: 800,
      positionY: 100
    },
    {
      order: 4,
      type: "OUTPUT",
      config: JSON.stringify({
        outputLayer: "SILVER",
        outputTable: "hr_enterprise_dirty_100_result",
        writeMode: "overwrite"
      }),
      outputLayer: "SILVER",
      outputTable: "hr_enterprise_dirty_100_result",
      positionX: 1100,
      positionY: 100
    }
  ];

  // 3. Create the new steps
  for (const step of steps) {
    await prisma.pipelineStep.create({
      data: {
        pipelineId,
        ...step
      }
    });
  }

  console.log("Successfully updated pipeline steps to use flag mode in database!");
}

main().catch(console.error).finally(() => prisma.$disconnect());
