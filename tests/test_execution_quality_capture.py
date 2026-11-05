"""Current native CLI record shapes, with neutral text and no processes."""

import copy
import json
import unittest
import uuid

from taskflow.execution_quality_capture import bind_increment


def encoded(records):
    return ("\n".join(json.dumps(r) for r in records) + "\n").encode()


class OwnedQualityCaptureTests(unittest.TestCase):
    def setUp(self):
        self.sid, self.turn = str(uuid.uuid4()), str(uuid.uuid4())
        self.request = b"Exact neutral request with a retained newline.\n"
        self.response = "Neutral observed response."
        metadata = {"turn_id":self.turn, "content_item_kinds":["user.text"]}
        self.records = [
            {"type":"session_meta", "payload":{"id":self.sid}},
            {"type":"event_msg", "payload":{"type":"task_started", "turn_id":self.turn}},
            {"type":"response_item", "payload":{"type":"message", "role":"user",
                "content":[{"type":"input_text", "text":self.request.decode()}],
                "internal_chat_message_metadata_passthrough":metadata}},
            {"type":"event_msg", "payload":{"type":"item_completed", "thread_id":self.sid, "turn_id":self.turn,
                "item":{"type":"UserMessage", "content":[{"type":"text", "text":self.request.decode(), "text_elements":[]}]}}},
            {"type":"event_msg", "payload":{"type":"item_completed", "thread_id":self.sid, "turn_id":self.turn,
                "item":{"type":"AgentMessage", "content":[{"type":"Text", "text":self.response}]}}},
            {"type":"event_msg", "payload":{"type":"task_complete", "turn_id":self.turn,
                                           "last_agent_message":self.response}},
        ]
        self.trace = [{"type":"thread.started", "thread_id":self.sid}, {"type":"turn.started"},
                      {"type":"item.completed", "item":{"type":"agent_message", "text":self.response}},
                      {"type":"turn.completed"}]

    def bind(self, records=None, previous=b"", request=None, trace=None):
        return bind_increment(encoded(records or self.records), previous,
                              self.request if request is None else request,
                              encoded(trace or self.trace), self.sid)

    def test_exact_fresh_and_resumed_increment(self):
        result = self.bind()
        self.assertEqual(self.turn,result['turn_id'])
        prior = copy.deepcopy(self.records)
        old_id = str(uuid.uuid4())
        for row in prior:
            payload = row.get('payload',{})
            if payload.get('turn_id'):payload['turn_id']=old_id
            if payload.get('internal_chat_message_metadata_passthrough'):
                payload['internal_chat_message_metadata_passthrough']['turn_id']=old_id
        prefix=encoded(prior)
        delta=[{"type":"event_msg","payload":{"type":"thread_settings_applied"}},*self.records[1:]]
        result=bind_increment(prefix+encoded(delta),prefix,self.request,encoded(self.trace),self.sid)
        self.assertEqual(len(prefix),result['previous_bytes'])

    def test_foreign_after_completion_or_extra_complete_rejected(self):
        for extra in (self.records[2],self.records[-1]):
            with self.subTest(extra=extra),self.assertRaises(ValueError):self.bind([*self.records,copy.deepcopy(extra)])

    def test_second_or_unattributed_user_inside_turn_rejected(self):
        for extra in (copy.deepcopy(self.records[2]),{'type':'response_item','payload':{'type':'message','role':'user','content':[]}}):
            with self.subTest(extra=extra),self.assertRaises(ValueError):self.bind([*self.records[:-1],extra,self.records[-1]])

    def test_exact_stdin_completion_and_item_identity_required(self):
        for target,field,value in ((2,'content',[{'type':'input_text','text':'replaced request'}]),
                                    (3,'turn_id',str(uuid.uuid4())),(5,'turn_id',str(uuid.uuid4())),
                                    (5,'last_agent_message','foreign response')):
            rows=copy.deepcopy(self.records);rows[target]['payload'][field]=value
            with self.subTest(field=field,target=target),self.assertRaises(ValueError):self.bind(rows)
        with self.assertRaises(ValueError):self.bind(request=b'different stdin')

    def test_old_turn_identity_and_missing_or_failed_visible_turn_rejected(self):
        with self.assertRaises(ValueError):self.bind([*self.records,*self.records[1:]],encoded(self.records))
        for trace in (self.trace[:-1],[*self.trace,{'type':'error'}]):
            with self.subTest(trace=trace),self.assertRaises(ValueError):self.bind(trace=trace)

    def test_extra_assistant_not_in_visible_trace_is_rejected(self):
        extra=copy.deepcopy(self.records[-2]);extra['payload']['item']['content'][0]['text']='foreign assistant'
        with self.assertRaises(ValueError):self.bind([*self.records[:-2],extra,*self.records[-2:]])

    def test_current_harness_is_bounded_and_attributed(self):
        harness=copy.deepcopy(self.records[2]);harness['payload']['content']=[{'type':'input_text','text':'neutral native environment'}]
        harness['payload']['internal_chat_message_metadata_passthrough']['content_item_kinds']=['environments.environment_context']
        self.bind([*self.records[:2],harness,*self.records[2:]])
        with self.assertRaises(ValueError):self.bind([*self.records[:2],harness,harness,*self.records[2:]])
        harness['payload']['internal_chat_message_metadata_passthrough']['content_item_kinds']=['foreign.instructions']
        with self.assertRaises(ValueError):self.bind([*self.records[:2],harness,*self.records[2:]])

    def test_prefix_or_session_identity_change_rejected(self):
        with self.assertRaises(ValueError):self.bind(previous=b'changed prefix\n')
        rows=copy.deepcopy(self.records);rows[0]['payload']['id']=str(uuid.uuid4())
        with self.assertRaises(ValueError):self.bind(rows)


if __name__ == '__main__':unittest.main()
