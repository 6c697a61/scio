/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.elasticsearch

import org.apache.http.HttpHost
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client._
import org.elasticsearch.client.indices.{CreateIndexRequest, CreateIndexResponse, GetIndexRequest}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType

import scala.collection.JavaConverters._
import scala.util.Try

object IndexAdmin {
  private def indicesClient[A](esOptions: ElasticsearchOptions)(f: IndicesClient => A): Try[A] = {
    val client = new RestHighLevelClient(RestClient.builder(esOptions.nodes: _*))

    val result = Try(f(client.indices()))
    client.close()
    result
  }

  /**
   * Ensure that index is created.
   * If index already exists or some other error occurs this results in a [[scala.util.Failure]].
   *
   * @param index index to be created
   * @param mappingSource a valid json string
   */
  def ensureIndex(
    nodes: Iterable[HttpHost],
    index: String,
    mappingSource: String
  ): Try[CreateIndexResponse] = {
    val esOptions = ElasticsearchOptions(nodes.toSeq)
    ensureIndex(esOptions, index, mappingSource)
  }

  /**
   * Ensure that index is created.
   * If index already exists or some other error occurs this results in a [[scala.util.Failure]].
   *
   * @param index index to be created
   * @param mappingSource a valid json string
   */
  def ensureIndex(
    esOptions: ElasticsearchOptions,
    index: String,
    mappingSource: String
  ): Try[CreateIndexResponse] =
    indicesClient(esOptions)(client => ensureIndex(index, mappingSource, client))

  /**
   * Ensure that index is created.
   *
   * @param index index to be created
   * @param mappingSource a valid json string
   */
  private def ensureIndex(
    index: String,
    mappingSource: String,
    client: IndicesClient
  ): CreateIndexResponse =
    client.create(
      new CreateIndexRequest(index).source(mappingSource, XContentType.JSON),
      RequestOptions.DEFAULT
    )

  def indexExists(nodes: Iterable[HttpHost], indexName: String): Try[Boolean] = {
    val esOptions = ElasticsearchOptions(nodes.toSeq)
    indicesClient(esOptions)(client =>
      client.exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)
    )
  }

  def indexExists(esOptions: ElasticsearchOptions, indexName: String): Try[Boolean] =
    indicesClient(esOptions)(client =>
      client.exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)
    )

  def indexExists(indexName: String, client: IndicesClient): Boolean =
    client.exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)

  def deleteIndex(nodes: Iterable[HttpHost], indexName: String): Try[AcknowledgedResponse] = {
    val request = new DeleteIndexRequest(indexName).timeout(TimeValue.timeValueSeconds(30))
    val esOptions = ElasticsearchOptions(nodes.toSeq)
    indicesClient(esOptions)(client => client.delete(request, RequestOptions.DEFAULT))
  }

  def deleteIndex(esOptions: ElasticsearchOptions, indexName: String): Try[AcknowledgedResponse] = {
    val request = new DeleteIndexRequest(indexName).timeout(TimeValue.timeValueSeconds(30))
    indicesClient(esOptions)(client => client.delete(request, RequestOptions.DEFAULT))
  }

  def deleteIndex(indexName: String, client: IndicesClient): AcknowledgedResponse = {
    val request = new DeleteIndexRequest(indexName).timeout(TimeValue.timeValueSeconds(30))
    client.delete(request, RequestOptions.DEFAULT)
  }

  /** Assigns alias to the index and unless removeExisting is set to false, removes alias from
   * all other indices it was assigned to */
  def updateAlias(
    nodes: Iterable[HttpHost],
    index: String,
    alias: String,
    removeExisting: Boolean = true
  ): Try[AcknowledgedResponse] = {
    val esOptions = ElasticsearchOptions(nodes.toSeq)
    updateAlias(esOptions, index, alias, removeExisting)
  }

  /** Assigns alias to the index and unless removeExisting is set to false, removes alias from
   * all other indices it was assigned to */
  def updateAlias(
    esOptions: ElasticsearchOptions,
    index: String,
    alias: String,
    removeExisting: Boolean = true
  ): Try[AcknowledgedResponse] = indicesClient(esOptions) { client =>
    updateAlias(index, alias, removeExisting, client)
  }

  /** Assigns alias to the index and unless removeExisting is set to false, removes alias from
   * all other indices it was assigned to */
  def updateAlias(
    index: String,
    alias: String,
    removeExisting: Boolean = true,
    client: IndicesClient
  ): AcknowledgedResponse = {
    val getAliasesResponse =
      client.getAlias(new GetAliasesRequest(alias), RequestOptions.DEFAULT)

    val request = new IndicesAliasesRequest()
      .addAliasAction(
        new AliasActions(AliasActions.Type.ADD)
          .index(index)
          .alias(alias)
      )

    if (removeExisting) {
      val indexAliacesToRemove =
        getAliasesResponse.getAliases.keySet().asScala.filterNot(_ == index)

      indexAliacesToRemove.foreach(indexName =>
        request.addAliasAction(
          new AliasActions(AliasActions.Type.REMOVE)
            .index(indexName)
            .alias(alias)
        )
      )
    }

    client.updateAliases(request, RequestOptions.DEFAULT);
  }
}
