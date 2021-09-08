import { Kafka, ITopicConfig, Consumer } from "kafkajs";
import {
  SagaDefinition,
  SagaMessage,
  STEP_PHASE,
} from "./sagaDefinitionBuilder";

const kafka = new Kafka({ brokers: ["localhost:9092"] });
const admin = kafka.admin();

export class SagaProcessor {
  producer = kafka.producer();
  consumer = kafka.consumer({ groupId: "saga" });

  constructor(private sagaDefinitions: SagaDefinition[]) {}

  async init() {
    await admin.connect();
    await this.producer.connect();
    await this.consumer.connect();

    const stepTopics = this.sagaDefinitions.map(
      (definition) => definition.channelName
    );

    // create all channels (topics) for all saga steps
    const kafkaTopics = stepTopics.map((topic): ITopicConfig => ({ topic }));
    await admin.createTopics({ topics: kafkaTopics });
    console.log("Saga Topics Created Successfully");

    // Subscribe to all created channels of all saga steps
    for (let topic of stepTopics) {
      /**
       * a topic instance may be payment service,
       * flight booking service, Hotel booking service
       */
      await this.consumer.subscribe({ topic });
    }

    await this.consumer.run({
      eachMessage: async ({ topic, message, partition }) => {
        const sagaMessage = JSON.parse(
          message.value!.toString()
        ) as SagaMessage;

        const { saga, payload } = sagaMessage;
        const { index, phase } = saga;

        console.log("=== message recieved", saga, payload);

        // Commit or rollback transaction
        switch (phase) {
          case STEP_PHASE.STEP_FORWARD: {
            const stepForward =
              this.sagaDefinitions[index].phases[STEP_PHASE.STEP_FORWARD]!
                .command;

            try {
              await stepForward();
              await this.makeStepForward(index + 1, payload);
            } catch (e) {
              await this.makeStepForward(index - 1, payload);
            }

            return;
          }

          case STEP_PHASE.STEP_BACK: {
            const stepBack =
              this.sagaDefinitions[index].phases[STEP_PHASE.STEP_BACK]!.command;
            try {
              await stepBack();
              await this.makeStepBack(index + 1, payload);
            } catch (e) {
              await this.makeStepBack(index - 1, payload);
            }

            return;
          }

          default: {
            console.log("SAGA PHASE UNAVAILABLE");
          }
        }
      },
    });
  }

  async makeStepForward(index: number, payload: any) {
    if (index >= this.sagaDefinitions.length) {
      console.log("====> Saga finished and transaction successful");
      return;
    }

    const message = {
      payload,
      saga: { index, phase: STEP_PHASE.STEP_FORWARD },
    };

    await this.producer.send({
      topic: this.sagaDefinitions[index].channelName,
      messages: [{ value: JSON.stringify(message) }],
    });
  }

  async makeStepBack(index: number, payload: any) {
    if (index < 0) {
      console.log("===> Saga finished and transaction rolled back");
      return;
    }

    await this.producer.send({
      topic: this.sagaDefinitions[index].channelName,
      messages: [
        {
          value: JSON.stringify({
            payload,
            saga: { index, phases: STEP_PHASE.STEP_BACK },
          }),
        },
      ],
    });
  }

  async start(payload: any) {
    await this.makeStepForward(0, payload);
    console.log("Saga Started");
  }
}
