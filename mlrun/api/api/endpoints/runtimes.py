import typing
from http import HTTPStatus

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session

import mlrun.api.crud
import mlrun.api.schemas
from mlrun.api.api import deps
from mlrun.config import config

router = APIRouter()


@router.get("/runtimes")
def list_runtimes(label_selector: str = None):
    return mlrun.api.crud.Runtimes().list_runtimes("*", label_selector)


# TODO: move everything to use this endpoint instead of list_runtimes and deprecate it
@router.get("/projects/{project}/runtime-resources")
def list_runtime_resources(
    project: str,
    label_selector: str = None,
    group_by: typing.Optional[
        mlrun.api.schemas.ListRuntimeResourcesGroupByField
    ] = Query(None, alias="group-by"),
):
    return mlrun.api.crud.Runtimes().list_runtimes(project, label_selector, group_by)


@router.get("/runtimes/{kind}")
def get_runtime(kind: str, label_selector: str = None):
    return mlrun.api.crud.Runtimes().get_runtime(kind, label_selector)


@router.delete("/runtimes", status_code=HTTPStatus.NO_CONTENT.value)
def delete_runtimes(
    label_selector: str = None,
    force: bool = False,
    grace_period: int = config.runtime_resources_deletion_grace_period,
    auth_verifier: deps.AuthVerifier = Depends(deps.AuthVerifier),
    db_session: Session = Depends(deps.get_db_session),
):
    mlrun.api.crud.Runtimes().delete_runtimes(
        db_session, label_selector, force, grace_period, auth_verifier.auth_info.session
    )
    return Response(status_code=HTTPStatus.NO_CONTENT.value)


@router.delete("/runtimes/{kind}", status_code=HTTPStatus.NO_CONTENT.value)
def delete_runtime(
    kind: str,
    label_selector: str = None,
    force: bool = False,
    grace_period: int = config.runtime_resources_deletion_grace_period,
    auth_verifier: deps.AuthVerifier = Depends(deps.AuthVerifier),
    db_session: Session = Depends(deps.get_db_session),
):
    mlrun.api.crud.Runtimes().delete_runtime(
        db_session,
        kind,
        label_selector,
        force,
        grace_period,
        auth_verifier.auth_info.session,
    )
    return Response(status_code=HTTPStatus.NO_CONTENT.value)


# FIXME: find a more REST-y path
@router.delete("/runtimes/{kind}/{object_id}", status_code=HTTPStatus.NO_CONTENT.value)
def delete_runtime_object(
    kind: str,
    object_id: str,
    label_selector: str = None,
    force: bool = False,
    grace_period: int = config.runtime_resources_deletion_grace_period,
    auth_verifier: deps.AuthVerifier = Depends(deps.AuthVerifier),
    db_session: Session = Depends(deps.get_db_session),
):
    mlrun.api.crud.Runtimes().delete_runtime_object(
        db_session,
        kind,
        object_id,
        label_selector,
        force,
        grace_period,
        auth_verifier.auth_info.session,
    )
    return Response(status_code=HTTPStatus.NO_CONTENT.value)
