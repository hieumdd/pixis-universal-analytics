import pytest

from ua import ua_service
# from tasks import tasks_service

TIME_FRAME = [
    # ("auto", (None, None)),
    ("manual", ("2022-03-01", "2022-06-01")),
]


@pytest.fixture(
    params=[i[1] for i in TIME_FRAME],
    ids=[i[0] for i in TIME_FRAME],
)
def timeframe(request):
    return request.param


class TestUniversalAnalytics:
    def test_get_transform_service(self, timeframe):
        res = ua_service._transform_service(
            ua_service._get_service(timeframe),
        )
        print(res)
        assert res

    def test_pipeline_service(self, timeframe):
        res = ua_service.pipeline_service(*timeframe)
        print(res)
        assert res


# class TestTasks:
#     def test_service(self, timeframe):
#         res = tasks_service.create_tasks_service(
#             {
#                 "start": timeframe[0],
#                 "end": timeframe[1],
#             }
#         )
#         print(res)
#         assert res["tasks"] > 0
