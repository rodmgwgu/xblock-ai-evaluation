"""
Celery task for student messages export.
"""
import itertools
import time

from celery import shared_task
from celery.utils.log import get_task_logger
from django.contrib.auth.models import User
from xblock.fields import Scope

from . import ShortAnswerAIEvalXBlock

logger = get_task_logger(__name__)


@shared_task()
def export_data(course_id_str):
    """
    Exports chat logs from all supported XBlocks.
    """
    from common.djangoapps.util.file import course_filename_prefix_generator
    from lms.djangoapps.instructor_task.models import ReportStore
    from opaque_keys.edx.keys import CourseKey

    start_timestamp = time.time()

    course_id = CourseKey.from_string(course_id_str)

    logger.debug("Beginning data export")

    header = ("Section", "Subsection", "Unit", "Location",
              "Display Name", "Username", "User E-mail", "Conversation",
              "Source", "Message")

    rows = itertools.chain([header], _extract_all_data(course_id))

    report_store = ReportStore.from_config(config_name='GRADES_DOWNLOAD')

    timestamp = time.strftime("%Y-%m-%d-%H%M%S", time.gmtime(start_timestamp))
    filename = "ai_eval_history-{course_prefix}-{timestamp_str}.csv".format(
        course_prefix=course_filename_prefix_generator(course_id),
        timestamp_str=timestamp
    )
    report_store.store_rows(course_id, filename, rows)

    generation_time_s = time.time() - start_timestamp
    logger.debug(f"Done data export - took {generation_time_s} seconds")

    return {
        "report_filename": filename,
        "start_timestamp": start_timestamp,
        "generation_time_s": generation_time_s,
    }


def _extract_all_data(course_id):
    from xmodule.modulestore.django import modulestore

    store = modulestore()
    for block in store.get_items(course_id):
        if isinstance(block, ShortAnswerAIEvalXBlock):
            yield from _extract_data(block)


def _extract_data(block):
    from lms.djangoapps.courseware.model_data import (
        DjangoKeyValueStore,
        FieldDataCache,
    )

    section_name, subsection_name, unit_name = _get_context(block)

    for user in User.objects.iterator():
        data = FieldDataCache([], block.course_id, user)
        data.add_blocks_to_cache([block])

        try:
            sessions = data.get(DjangoKeyValueStore.Key(
                scope=Scope.user_state,
                user_id=user.id,
                block_scope_id=block.location,
                field_name='sessions'
            ))
        except KeyError:
            continue

        for i, conversation in enumerate(sessions, start=1):
            for message in conversation:
                yield (
                    section_name,
                    subsection_name,
                    unit_name,
                    str(block.location),
                    block.display_name,
                    user.username,
                    user.email or "",
                    i,
                    message["source"],
                    message["content"]
                )


def _get_context(block):
    """
    Return section, subsection, and unit names for `block`.
    """
    block_names_by_type = {}
    block_iter = block
    while block_iter:
        block_iter_type = block_iter.scope_ids.block_type
        block_names_by_type[block_iter_type] = block_iter.display_name_with_default
        block_iter = block_iter.get_parent() if block_iter.parent else None
    section_name = block_names_by_type.get('chapter', '')
    subsection_name = block_names_by_type.get('sequential', '')
    unit_name = block_names_by_type.get('vertical', '')
    return section_name, subsection_name, unit_name