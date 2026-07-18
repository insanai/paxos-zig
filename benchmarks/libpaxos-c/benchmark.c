#include "acceptor.h"
#include "learner.h"
#include "paxos.h"
#include "proposer.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

enum {
    node_count = 3,
    value_count = 4096,
    sample_count = 7,
    measurement_iterations = 32,
    preexecution_window = 128,
};

struct sample {
    uint64_t checksum;
    uint64_t messages;
    uint64_t nanoseconds;
};

static void fail(const char *message)
{
    fprintf(stderr, "libpaxos benchmark: %s\n", message);
    exit(1);
}

static uint64_t now_nanoseconds(void)
{
    struct timespec time;
    if (clock_gettime(CLOCK_MONOTONIC, &time) != 0)
        fail("clock_gettime failed");
    return (uint64_t)time.tv_sec * 1000000000ULL + (uint64_t)time.tv_nsec;
}

static void prepare_one(struct proposer *proposer, struct acceptor **acceptors,
    uint64_t *messages)
{
    paxos_prepare prepare;
    proposer_prepare(proposer, &prepare);

    for (int index = 0; index < node_count; index++) {
        paxos_message reply = {0};
        paxos_prepare retry = {0};
        *messages += 1; /* prepare delivered to one acceptor */
        if (!acceptor_receive_prepare(acceptors[index], &prepare, &reply))
            fail("acceptor rejected a fresh prepare");
        if (reply.type != PAXOS_PROMISE)
            fail("prepare did not produce a promise");
        *messages += 1; /* promise returned to the proposer */
        if (proposer_receive_promise(proposer, &reply.u.promise, &retry))
            fail("fresh prepare was preempted");
        paxos_message_destroy(&reply);
    }
}

static struct sample run_sample(void)
{
    struct proposer *proposer = proposer_new(0, node_count);
    struct learner *learner = learner_new(node_count);
    struct acceptor *acceptors[node_count];
    uint64_t setup_messages = 0;
    uint64_t messages = 0;
    for (int index = 0; index < node_count; index++)
        acceptors[index] = acceptor_new(index);
    for (int index = 0; index < preexecution_window; index++)
        prepare_one(proposer, acceptors, &setup_messages);

    uint64_t checksum = 0;
    const uint64_t started = now_nanoseconds();
    for (uint64_t value = 1; value <= value_count; value++) {
        paxos_accept accept = {0};
        paxos_message replies[node_count];
        proposer_propose(proposer, (const char *)&value, sizeof(value));
        if (!proposer_accept(proposer, &accept))
            fail("prepared instance was unavailable");

        for (int index = 0; index < node_count; index++) {
            replies[index] = (paxos_message){0};
            messages += 1; /* accept delivered to one acceptor */
            if (!acceptor_receive_accept(acceptors[index], &accept, &replies[index]))
                fail("acceptor rejected a prepared accept");
            if (replies[index].type != PAXOS_ACCEPTED)
                fail("accept did not produce an accepted message");
        }
        for (int index = 0; index < node_count; index++) {
            messages += 1; /* accepted returned to the co-located learner+proposer */
            learner_receive_accepted(learner, &replies[index].u.accepted);
            proposer_receive_accepted(proposer, &replies[index].u.accepted);
            paxos_message_destroy(&replies[index]);
        }

        paxos_accepted delivered = {0};
        if (!learner_deliver_next(learner, &delivered))
            fail("chosen value was not delivered");
        if (delivered.value.paxos_value_len != (int)sizeof(uint64_t))
            fail("delivered value has the wrong size");
        checksum += *(const uint64_t *)delivered.value.paxos_value_val;
        paxos_accepted_destroy(&delivered);
        /* The phase-one preexecution window advances inside the timed
         * region: LibPaxos3 pays prepare+promise per value here, a heavier
         * protocol path than the stable-leader 6-message path. */
        prepare_one(proposer, acceptors, &messages);
    }
    const uint64_t elapsed = now_nanoseconds() - started;

    proposer_free(proposer);
    learner_free(learner);
    for (int index = 0; index < node_count; index++)
        acceptor_free(acceptors[index]);

    return (struct sample){
        .checksum = checksum,
        .messages = messages,
        .nanoseconds = elapsed,
    };
}

static struct sample run_measurement(void)
{
    struct sample result = {0};
    for (int iteration = 0; iteration < measurement_iterations; iteration++) {
        const struct sample sample = run_sample();
        result.checksum += sample.checksum;
        result.messages += sample.messages;
        result.nanoseconds += sample.nanoseconds;
    }
    return result;
}

static int compare_samples(const void *left, const void *right)
{
    const struct sample *a = left;
    const struct sample *b = right;
    if (a->nanoseconds < b->nanoseconds)
        return -1;
    if (a->nanoseconds > b->nanoseconds)
        return 1;
    return 0;
}

int main(void)
{
    paxos_config.verbosity = PAXOS_LOG_ERROR;
    struct sample samples[sample_count];
    for (int index = 0; index < sample_count; index++)
        samples[index] = run_measurement();
    qsort(samples, sample_count, sizeof(samples[0]), compare_samples);
    const struct sample result = samples[sample_count / 2];

    printf("implementation: LibPaxos3 d255f8b (C)\n");
    printf("path:           per-value accept plus phase-one preexecution;\n");
    printf("                heavier than the 6-message stable-leader path\n");
    const uint64_t measured_values = (uint64_t)value_count * measurement_iterations;
    printf("values:         %" PRIu64 " (%d x %d iterations)\n",
        measured_values, value_count, measurement_iterations);
    printf("median_ns:      %" PRIu64 "\n", result.nanoseconds);
    printf("range_ns:       %" PRIu64 "..%" PRIu64 " across %d samples\n",
        samples[0].nanoseconds, samples[sample_count - 1].nanoseconds,
        sample_count);
    printf("ns_per_value:   %.2f\n", (double)result.nanoseconds / measured_values);
    printf("messages:       %" PRIu64 " (measured)\n", result.messages);
    printf("messages/value: %.2f\n", (double)result.messages / measured_values);
    printf("checksum:       %" PRIu64 "\n", result.checksum);
    printf("{\"impl\":\"libpaxos3\",\"workload\":\"u64-3n\","
           "\"mode\":\"sync-preexec\",\"values\":%" PRIu64 ",\"nodes\":%d,"
           "\"payload_bytes\":8,\"values_per_iteration\":%d,"
           "\"measurement_iterations\":%d,\"ns_total_median\":%" PRIu64 ","
           "\"ns_total_min\":%" PRIu64 ",\"ns_total_max\":%" PRIu64 ","
           "\"ns_per_value\":%.2f,\"messages\":%" PRIu64 ","
           "\"checksum\":%" PRIu64 "}\n\n",
        measured_values, node_count, value_count, measurement_iterations,
        result.nanoseconds, samples[0].nanoseconds,
        samples[sample_count - 1].nanoseconds,
        (double)result.nanoseconds / measured_values, result.messages,
        result.checksum);
    const uint64_t expected = (uint64_t)value_count * (value_count + 1) / 2 *
        measurement_iterations;
    const uint64_t expected_messages = measured_values * 12;
    if (result.checksum != expected || result.messages != expected_messages) {
        fprintf(stderr, "SELF-CHECK FAILED: checksum or message mismatch\n");
        return 1;
    }
    return 0;
}
