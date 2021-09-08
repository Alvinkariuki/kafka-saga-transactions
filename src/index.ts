import { SagaDefinitionBuilder } from "./saga/sagaDefinitionBuilder";

async function run() {
  const sagaDefinitionBuilder = new SagaDefinitionBuilder()
    .step("FlightBookingService")
    .onReply(async () => {
      // invoke Flight Booking Service API to reserve flight ticket
      console.log("STEP1 FOWARD");
    })
    .withCompensation(async () => {
      // invoke Flight Booking Service API to rollback reserved ticket
      console.log("STEP1 COMPENSATION");
    })
    .step("HotelBookingService")
    .onReply(async () => {
      // invoke Hotel Booking Service API to reserve flight ticket
      console.log("STEP2 FOWARD");
    })
    .withCompensation(async () => {
      // invoke Hotel Booking Service API to rollback reserved ticket
      console.log("STEP2 COMPENSATION");
    })
    .step("PaymentService")
    .onReply(async () => {
      // invoke Payment Service API to reserve flight ticket
      console.log("STEP3 FOWARD");
    })
    .withCompensation(async () => {
      // invoke Payment Service API to rollback reserved ticket
      console.log("STEP3 COMPENSATION");
    });

  const sagaProcessor = await sagaDefinitionBuilder.build();

  sagaProcessor.start({ id: 1 });
}
