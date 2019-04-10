package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mcbs/trace"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type AggregatorConfiguration struct {
	Name                  string `json:"name"`
	CorrelationExpression string `json:"correlationExpression"`
	CompletionCondition   string `json:"completionCondition"`
	CompletionValue       int    `json:"completionValue"`
	Collectors            string `json:"collectors"`
	CollectorType         string `json:"collectorType"`
	Destination           string `json:"destination"`
	DestinationType       string `json:"destinationType"`
	AggregationStrategy   string `json:"aggregationStrategy"`
}

type AggsList struct {
	Aggregations []string `json:"aggregations"`
}

type AggregatorController struct {
	aggDb                                       *sql.DB
	aggGetConfigStmt                            *sql.Stmt
	createNewAggregation                        *sql.Stmt
	createNewAggregationInstance                *sql.Stmt
	addMessageToAggInstance                     *sql.Stmt
	checkAggInstanceExists                      *sql.Stmt
	countNumMessages                            *sql.Stmt
	getMessages                                 *sql.Stmt
	deleteMessages                              *sql.Stmt
	getAggregations                             *sql.Stmt
	deleteAggregationStmt                       *sql.Stmt
	checkAggExistStmt                           *sql.Stmt
	updateAggInstance                           *sql.Stmt
	retrieveCCandVal                            *sql.Stmt
	DeleteAllInstancesAssociatedWithAggregation *sql.Stmt
	config                                      []AggregatorConfiguration
	queueList                                   map[string]int
	router                                      *mux.Router
	listenOnREST                                bool
}

/**
 *  getAggregationConfig retrieves aggregation information from the database
 */
func (ac *AggregatorController) getAggregationConfig() {
	logger.LogInfo("Entering getAggregationConfig()")
	var err error
	logger.LogInfo("Retrieving aggregation configuration")
	if ac.config, err = retrieveAggregationConfiguration(ac.aggGetConfigStmt); err != nil {
		logger.LogError(err.Error())
	}
	logger.LogInfo("Exiting getAggregationConfig()")
	return
}

/**
 *  Registers REST HTTP function handlers with router
 */
func (ac *AggregatorController) registerRouter() {
	logger.LogInfo("Entering registerRouter()")
	ac.router = mux.NewRouter().StrictSlash(true)
	ac.router.HandleFunc("/collect", ac.handleCollectMessage).Methods("POST")
	ac.router.HandleFunc("/createAggregation", ac.handleCreateAggregation).Methods("POST")
	ac.router.HandleFunc("/getListOfAggregations", ac.retreiveListOfAggregations).Methods("GET")
	ac.router.HandleFunc("/deleteAggregation", ac.deleteAggregation).Methods("DELETE")
	ac.router.HandleFunc("/completionCondition", ac.updateCompletionCondition).Methods("PATCH")
	ac.router.Handle("/metrics", promhttp.Handler())
	logger.LogInfo("Exiting registerRouter()")
}

/**
 *  aggDelete issues a database call to delete aggregation from the database
 */
func (ac *AggregatorController) aggDelete(ctx context.Context, name string) bool {
	logger.LogInfo("Entering aggDelete()")
	// start opentracing child span
	span, ctx, _ := trace.StartSpanFromContext(ctx, "aggDelete")
	defer span.Finish()

	err := deleteAgg(ac.deleteAggregationStmt, name)
	if err != nil {
		logger.LogError("Failed to delete aggregation from database: ", err.Error())
		logger.LogInfo("Exiting aggDelete()")
		return false
	}

	logger.LogError(fmt.Sprintf("Successfully delete aggregation: %s from database.", name))
	ac.deleteFromConfigArray(ctx, name)

	err = deleteAggInstancesAssocWithAggregation(ac.DeleteAllInstancesAssociatedWithAggregation, name)
	if err != nil {
		logger.LogError("Failed to delete aggregation instances associated with deleted aggregation.")
		logger.LogInfo("Exiting aggDelete()")
		return false
	}

	logger.LogInfo("Exiting aggDelete()")
	return true
}

/**
 *  deleteFromConfigArray updates the config array after a delete
 */
func (ac *AggregatorController) deleteFromConfigArray(ctx context.Context, name string) {
	logger.LogInfo("Entering deletFromConfigArray()")
	// start opentracing child span
	span, ctx, _ := trace.StartSpanFromContext(ctx, "deleteFromConfigArray")
	defer span.Finish()

	var i int
	var item_to_delete int = -1
	for i = 0; i < len(ac.config); i++ {
		if strings.Compare(ac.config[i].Name, name) == 0 {
			item_to_delete = i
		}
	}

	if item_to_delete != -1 {
		ac.config = ac.removeFromArray(ac.config, item_to_delete)
	}
	logger.LogInfo("Exiting deleteFromConfigArray()")
}

/**
 * removeFromArray removes an element from the array and returns the array
 */
func (ac *AggregatorController) removeFromArray(configs []AggregatorConfiguration, val int) []AggregatorConfiguration {
	logger.LogInfo("Entering removeFromArray()")
	configs[val] = configs[len(configs)-1]
	logger.LogInfo("Exiting removeFromArray")
	return configs[:len(configs)-1]
}

/**
 *  deleteAggregation handles delete aggregation requests, gets aggregation name from URL query parameter
 */
func (ac *AggregatorController) deleteAggregation(w http.ResponseWriter, r *http.Request) {
	logger.LogInfo("Entering deleteAggregation()")
	// start opentracing child span
	span, _ := trace.SpanFromHttpReq(r, "deleteAggregation")
	ctx := opentracing.ContextWithSpan(context.Background(), span)
	defer span.Finish()

	aggName := r.URL.Query().Get("aggregation")
	if aggName != "" {
		if ac.aggDelete(ctx, aggName) == true {
			fmt.Fprintf(w, "{\"status\" : \"success\"}")
		} else {
			fmt.Fprintf(w, "{\"status\" : \"error\"}")
		}
	} else {
		logger.LogError("No aggregation name specified in the URL query")
		fmt.Fprintf(w, "{\"status\" : \"error\"}")
	}
	logger.LogInfo("Exiting deleteAggregation()")
}

/**
 *  retrieveListOfAggregations retrieves list of aggregations in JSON format
 */
func (ac *AggregatorController) retreiveListOfAggregations(w http.ResponseWriter, r *http.Request) {
	logger.LogInfo("Entering retrieveListOfAggregations()")
	// start opentracing child span
	span, _ := trace.SpanFromHttpReq(r, "retreiveListOfAggregations")
	defer span.Finish()

	aggs, err := getAggregationsInDatabase(ac.getAggregations)

	if err != nil {
		fmt.Fprintf(w, "{\"status\" : \"error\"}")
	}

	var aList AggsList

	aList.Aggregations = aggs
	json.NewEncoder(w).Encode(aList)
	logger.LogInfo("Exiting retrieveListOfAggregations()")
}

/**
 *  listen listens on port 8080 with the configured router
 */
func (ac *AggregatorController) listen() {
	http.ListenAndServe(":8080", ac.router)
}

/**
 * chckInstanceExists checks if the aggregation instance with aggregation name and correlation id exists in
 * the database.
 */
func (ac *AggregatorController) chckInstanceExists(ctx context.Context, name string, correlation string) bool {
	logger.LogInfo("Entering chckInstanceExists()")
	// start opentracing child span
	span, ctx, _ := trace.StartSpanFromContext(ctx, "chckInstanceExists")
	defer span.Finish()

	res, err := checkAggregationInstanceExists(ac.checkAggInstanceExists, name, correlation)
	if err != nil {
		logger.LogInfo(err.Error())
		logger.LogInfo("Exiting chckInstanceExists()")
		return false
	}
	logger.LogInfo("Exiting chckInstanceExists()")
	return res
}

// chckAggregationExists checks is the aggregation instance exists
func (ac *AggregatorController) chckAggregationExists(ctx context.Context, name string) bool {
	logger.LogInfo("Entering chckAggregationExists()")
	// start opentracing child span
	span, _, _ := trace.StartSpanFromContext(ctx, "chckAggregationExists")
	defer span.Finish()

	res, err := checkAggExists(ac.checkAggExistStmt, name)
	if err != nil {
		logger.LogInfo(err.Error())
		logger.LogInfo("Exiting chckAggregationExists()")
		return false
	}
	logger.LogInfo("Exiting chckAggregationExists()")
	return res
}

/**
 * Creates a new aggregation instance
 */
func (ac *AggregatorController) createAggInst(ctx context.Context, name string, correlation string) bool {
	logger.LogInfo("Entering createAggInst()")
	// start opentracing child span
	span, _, _ := trace.StartSpanFromContext(ctx, "createAggInst")
	defer span.Finish()

	logger.LogInfo("Creating new aggregation instance")
	if err := createNewAggregationInst(ac.createNewAggregationInstance, name, correlation); err != nil {
		logger.LogInfo(err.Error())
		logger.LogInfo("Failed to create new aggregation instance")
		logger.LogInfo("Exiting createAggInst()")
		return false
	}
	logger.LogInfo("Exiting createAggInst()")
	return true
}

/**
 * Adds a message to an existing instance
 */
func (ac *AggregatorController) addMsgToInst(ctx context.Context, name string, correlation string, msg string) {
	logger.LogInfo("Entering addMsgToInst()")
	// start opentracing child span
	span, _, _ := trace.StartSpanFromContext(ctx, "addMsgToInst")
	defer span.Finish()

	logger.LogInfo(fmt.Sprintf("Adding message to existing Aggregator instance: %s with correlation ID %s and name %s", msg, correlation, name))
	if err := addMessageToAggInst(ac.addMessageToAggInstance, msg, name, correlation); err != nil {
		logger.LogInfo("Failed to add message to correlation instance: ", err.Error())
		logger.LogInfo("Exiting addMsgToInst()")
		return
	}
	logger.LogInfo("Success")
	logger.LogInfo("Exiting addMsg()")
}

/**
 * Updates aggregation instance with completion condition
 */
func (ac *AggregatorController) updateAggInst(ctx context.Context, name string, correlation string, completionCondition string, completionValue int) bool {
	logger.LogInfo("Entering updateAggInst()")
	// start opentracing child span
	span, _, _ := trace.StartSpanFromContext(ctx, "updateAggInstance")
	defer span.Finish()

	if err := updateAggInstWithConditions(ac.updateAggInstance, name, correlation, completionCondition, completionValue); err != nil {
		logger.LogInfo("failed to update aggregation instance with specific completion conditions: ", err.Error())
		logger.LogInfo("Exiting updateAggInst()")
		return false
	}

	logger.LogInfo("Success")
	logger.LogInfo("Exiting updateAggInst()")
	return true
}

/**
 * Get completion condition and value
 */
func (ac *AggregatorController) getCompletionConditionAndValue(ctx context.Context, name string, corrId string) (string, int, error) {
	logger.LogInfo("getCompletionContitionAndValue()")
	// start opentracing child span
	span, _, _ := trace.StartSpanFromContext(ctx, "getCompletionConditionAndValue")
	defer span.Finish()

	condition, val, err := getCCandVal(ac.retrieveCCandVal, name, corrId)
	if err != nil {
		logger.LogInfo("Exiting getCompletionConditionAndValue()")
		return "", -1, err
	}
	logger.LogInfo("Exiting getCompletionConditionAndValue()")
	return condition, val, nil
}

/**
 *  updateCompletionCondition updates the completion condition for a specific aggregation instance, be it
 *  size or timeout length.
 */
type completionCondition struct {
	Name                string `json:"name"`
	CorrelationID       int    `json:"correlationID"`
	CompletionCondition string `json:"completionCondition"`
	CompletionValue     int    `json:"completionValue"`
}

func (ac *AggregatorController) updateCompletionCondition(w http.ResponseWriter, r *http.Request) {
	logger.LogInfo("Entering updateCompletionCondition()")
	// start opentracing child span
	span, _ := trace.SpanFromHttpReq(r, "updateCompletionCondition")
	ctx := opentracing.ContextWithSpan(context.Background(), span)
	defer span.Finish()

	var cc completionCondition
	json.NewDecoder(r.Body).Decode(&cc)

	correlation_id := strconv.Itoa(cc.CorrelationID)
	if exists := ac.chckInstanceExists(ctx, cc.Name, correlation_id); exists == false {
		if ac.createAggInst(ctx, cc.Name, correlation_id) == false {
			logger.LogInfo("Unable to create new aggregation instance for non-default completion conditions")
			fmt.Fprintf(w, "{\"status\" : \"error\"}")
			logger.LogInfo("Exiting updateCompletionCondition()")
			return
		}
	}

	if ac.updateAggInst(ctx, cc.Name, correlation_id, cc.CompletionCondition, cc.CompletionValue) {
		logger.LogInfo("Updated aggregation instance with new conditions")
		fmt.Fprintf(w, "{\"status\" : \"success\"}")
	} else {
		fmt.Fprintf(w, "{\"status\" : \"error\"}")
	}
	logger.LogInfo("Exiting updateCompletionCondiditon()")
}

/**
 * handleCollectMessage is the entrypoint function to handle the /collect REST endpoint
 */
func (ac *AggregatorController) handleCollectMessage(w http.ResponseWriter, r *http.Request) {
	logger.LogInfo("Exiting handleCollectMessage()")
	// start opentracing child span
	span, _ := trace.SpanFromHttpReq(r, "handleCollectMessage")
	ctx := opentracing.ContextWithSpan(context.Background(), span)
	defer span.Finish()

	correlationID := r.Header.Get("Correlation-Id")
	aggName := r.Header.Get("Aggregation-Name")

	logger.LogInfo("Received collect request")

	res := ac.chckAggregationExists(ctx, aggName)

	if res == false {
		logger.LogInfo("Aggregation does not exist")
		fmt.Fprintf(w, "{\"status\" : \"Aggregation of name %s does not exist.\"}", aggName)
		logger.LogInfo("Exiting handleCollectMessage()")
		return
	}

	res = ac.chckInstanceExists(ctx, aggName, correlationID)

	if res == false {
		logger.LogInfo("Existing aggregation instance does not exist")
		if ac.createAggInst(ctx, aggName, correlationID) == false {
			logger.LogInfo("Unable to create new aggregation instance")
			fmt.Fprintf(w, "{\"status\" : \"error\"}")
			logger.LogInfo("Exiting handleCollectMessage()")
			return
		}

		logger.LogInfo("Successfully created new aggregation instance")
	}

	logger.LogInfo("Updating existing aggregation instance")
	myStr, err := ioutil.ReadAll(r.Body)
	msg := string(myStr)

	if err != nil {
		fmt.Fprintf(w, "{\"status\" : \"error\"}")
		logger.LogInfo("Exiting handleCollectMessage()")
		return
	}

	ac.addMsgToInst(ctx, aggName, correlationID, msg)

	/* Get the completion-condition and completion-value from the instance */
	complcond, complval, err := ac.getCompletionConditionAndValue(ctx, aggName, correlationID)

	if err != nil {
		logger.LogInfo("Error retreiving completion conditions and values")
		fmt.Fprintf(w, "{\"status\" : \"error\"}")
		logger.LogInfo("Exiting handleCollectionMessage()")
		return
	}

	if complval == -1 { /* This is the default setting, use the aggregations' completion value */
		/* The message has been added.  Now we must check the completion condition
		   and collect and send off the messages if complete */
		logger.LogInfo("Using default completion condition configured in aggregation")
		_, cmplmsg := ac.checkAndProcessCompletionConditions(ctx, aggName, correlationID, false, -1, "")

		fmt.Fprintf(w, "{\"status\" : \"%s\"}", cmplmsg)
	} else { /* This has been set, this will take precedence over the existing completion value in the
		   associated aggregation */
		logger.LogInfo("Using custom completion condition configured in aggregation instance")
		_, cmplmsg := ac.checkAndProcessCompletionConditions(ctx, aggName, correlationID, true, complval, complcond)

		fmt.Fprintf(w, "{\"status\" : \"%s\"}", cmplmsg)
	}
	logger.LogInfo("Exiting handleCollectionMessage()")
}

/**
 *  getNumberOfMessagesReceived returns number of messages received in the current aggregation instance
 */
func (ac *AggregatorController) getNumberOfMessagesReceived(name string, correlation string) int {
	var msgs int
	logger.LogInfo("Entering getNumberOfMessagesReceived()")

	msgs, err := getMsgCount(ac.countNumMessages, name, correlation)

	if err != nil {
		logger.LogInfo("Error retrieving message count: ", err.Error())
		logger.LogInfo("Exiting getNumberOfMessagesReceived()")
		return -1
	}
	logger.LogInfo("Exiting getNumberOfMessagesReceived()")
	return msgs
}

/**
 *  getAllMessagesFromInstance returns list of message from the aggregation instance
 */
func (ac *AggregatorController) getAllMessagesFromInstance(ctx context.Context, aggName string, correlationId string) *[]map[string]interface{} {
	logger.LogInfo("Entering getAllMessagesFromInstance()")
	// start opentracing child span
	span, ctx, _ := trace.StartSpanFromContext(ctx, "getAllMessagesFromInstance")
	defer span.Finish()

	allmsgs, err := getAllMessages(ac.getMessages, aggName, correlationId)

	if err != nil {
		logger.LogInfo("Error getting all messages: ", err.Error())
		logger.LogInfo("Exiting getAllMessagesFromInstance()")
		return nil
	} else {
		logger.LogInfo("Retrieved messages, processing ...")
	}

	var msgJsons []map[string]interface{}

	var i int
	for i = 0; i < len(allmsgs); i++ {
		var msgJson map[string]interface{}
		err = json.Unmarshal([]byte(allmsgs[i]), &msgJson)

		logger.LogInfo(fmt.Sprintf("Unmarshalling %s into interface", allmsgs[i]))

		if err != nil {
			logger.LogError("Error unmarshalling messages into interface: " + err.Error())
			logger.LogInfo("Exiting getAllMessagesFromInstance()")
			return nil
		}

		msgJsons = append(msgJsons, msgJson)
	}

	logger.LogInfo("Successfully unmarshalled messages into interface")
	logger.LogInfo("Exiting getAllMessagesFromInstance()")
	return &msgJsons
}

/**
 *  deleteInstance deletes aggregation instance from the database
 */
func (ac *AggregatorController) deleteInstance(ctx context.Context, aggName string, correlationId string) {
	logger.LogInfo("Entering deleteInstance()")
	// start opentracing child span
	span, ctx, _ := trace.StartSpanFromContext(ctx, "deleteInstance")
	defer span.Finish()

	err := deleteMessages(ac.deleteMessages, aggName, correlationId)

	if err != nil {
		logger.LogInfo("Error deleting all messages from aggregation instance: ", err.Error())
	}
	logger.LogInfo("Exiting deleteInstance()")
}

/**
 * postMsgToConfiguredEndpoint posts aggregated messages to configured URL endpoint
 */
func (ac *AggregatorController) postMsgToConfiguredEndpoint(ctx context.Context, msg []byte, url string) {
	logger.LogInfo("Exiting postMsgToConfiguredEndpoint()")
	// start opentracing child span
	span, ctx, _ := trace.StartSpanFromContext(ctx, "postMsgToConfiguredEndpoint")
	defer span.Finish()

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(msg)

	msgurl := fmt.Sprintf("%s", url)

	req, err := http.NewRequest("POST", msgurl, b)

	if err != nil {
		logger.LogError(fmt.Sprintf("Failure generating new POST request to url: %s", url))
		logger.LogInfo("Exiting postMsgToConfiguredEndpoint()")
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// inject opentracing span context into the request header
	trace.InjectSpanInHTTPHeader(ctx, req)

	client := &http.Client{
		Timeout: time.Second * 60,
	}

	res, err := client.Do(req)

	if err != nil {
		logger.LogError(fmt.Sprintf("Error POST'ing aggregated messages to url %s : %s", url, err.Error()))
		logger.LogInfo("Exiting postMsgToConfiguredEndpoint()")
		return
	}

	defer res.Body.Close()

	if res.StatusCode == 200 {
		logger.LogError("Service returned status code: ", res.StatusCode)
	} else {
		logger.LogInfo("Message succesfully received by configured endpoint")
	}
	logger.LogInfo("Exiting postMsgToConfiguredEndpoint()")
}

/**
 *  checkAndProcessCompletionConditions checks the completion conditions configured for the given aggregation and proceeds to evaluate
 *  , execute and finalize the aggregated transactions
 */
func (ac *AggregatorController) checkAndProcessCompletionConditions(ctx context.Context, aggregationName string, correlationName string, useInstanceCompletionConfig bool, instComplVal int, instComplCond string) (bool, string) {
	logger.LogInfo("Entering checkAndProcessCompletionConditions()")
	// start opentracing child span
	span, ctx, _ := trace.StartSpanFromContext(ctx, "checkAndProcessCompletionConditions")
	defer span.Finish()

	var thisConfig *AggregatorConfiguration

	var completionValueToCheck int
	var completionConditionToCheck string

	if thisConfig = ac.getAggregatorConfigByName(aggregationName); thisConfig != nil {
		/* Determine which completion conditions and completion values to check base on parameter */
		if useInstanceCompletionConfig {
			completionValueToCheck = instComplVal
			completionConditionToCheck = instComplCond
		} else {
			completionValueToCheck = thisConfig.CompletionValue
			completionConditionToCheck = thisConfig.CompletionCondition
		}

		logger.LogInfo("Retrieved the configuration, proceeding")
		if strings.Compare(completionConditionToCheck, "completion-size") == 0 {
			if msgs := ac.getNumberOfMessagesReceived(aggregationName, correlationName); msgs != -1 {
				if msgs < completionValueToCheck {
					logger.LogInfo("Waiting for more messages")
					logger.LogInfo("Exiting checkAndProcessCompletionConditions()")
					return true, "Waiting for more messages"
				} else {
					logger.LogInfo("All messages received, proceeding to cleanup and send to destination.")
					allmsgs := ac.getAllMessagesFromInstance(ctx, aggregationName, correlationName)

					if allmsgs != nil {
						logger.LogInfo(fmt.Sprintf("All messages : %v", *allmsgs))
						ac.deleteInstance(ctx, aggregationName, correlationName)
						MSG, err := json.Marshal(allmsgs)
						if err != nil {
							logger.LogError("Error marshalling messages list into JSON")
						} else {
							logger.LogInfo(string(MSG))
						}
						//finalMSG := MSG//string(MSG)
						logger.LogInfo(fmt.Sprintf("This is the message we are sending to the destination %s: %s", thisConfig.Destination, MSG))
						if strings.Compare(thisConfig.DestinationType, "QUEUE") == 0 {
							/* Enqueue to destination queue */
							globalActiveMQBroker.sendMessage(ctx, thisConfig.Destination, MSG, aggregationName, correlationName)
							logger.LogInfo("Exiting checkAndProcessCompletionConditions()")
							return true, "success"
						} else if strings.Compare(thisConfig.DestinationType, "URL") == 0 {
							/* POST messages to REST destination */
							ac.postMsgToConfiguredEndpoint(ctx, MSG, thisConfig.Destination)
							logger.LogInfo("Exiting getAllMessagesFromInstance()")
							return true, "success"
						} else {
							logger.LogInfo("Exiting getAllMessagesFromInstance()")
							return false, "Undefined destination type"
						}
					} else {
						logger.LogInfo("Exiting getAllMessagesFromInstance()")
						return false, "nil message received"
					}
				}
			} else {
				logger.LogError("Could get the total number of messages received in aggregation instance")
				logger.LogInfo("Exiting getAllMessagesFromInstance()")
				return false, "Could get the total number of messages received in aggregation instance"
			}
		} else {
			logger.LogError(fmt.Sprintf("Configuration completion condition: %s is invalid", completionConditionToCheck))
			logger.LogInfo("Exiting getAlklMessagesFromInstance()")
			return false, "Configuration completion condition is invalid"
		}
	} else {
		logger.LogError("No Aggregator Configurations exist")
		logger.LogInfo("Exiting getAllMessagesFromInstance()")
		return false, "No Aggregator Configurations exist"
	}
}

/**
 *  getAggregatorConfitByName retrieves the aggregator configuration object associated to this specific aggregation name
 */
func (ac *AggregatorController) getAggregatorConfigByName(aggName string) *AggregatorConfiguration {
	var i int
	logger.LogInfo("Entering getAggregatorConfigByName()")

	for i = 0; i < len(ac.config); i++ {
		if strings.Compare(ac.config[i].Name, aggName) == 0 {
			logger.LogInfo("Entering getAggregatorConfigByName()")
			return &ac.config[i]
		}
	}
	logger.LogInfo("Entering getAggregatorConfigByName()")
	return nil
}

/**
 *  handleCreateAggregation is a handler for configuration creation REST endpoint
 */
func (ac *AggregatorController) handleCreateAggregation(w http.ResponseWriter, r *http.Request) {
	logger.LogInfo("Entering handleCreateAggregation()")
	// start opentracing child span
	span, _ := trace.SpanFromHttpReq(r, "handleCreateAggregation")
	defer span.Finish()

	var configu AggregatorConfiguration
	var i int
	json.NewDecoder(r.Body).Decode(&configu)
	logger.LogInfo(fmt.Sprintf("%+v", configu))

	for i = 0; i < len(ac.config); i++ {
		if strings.Compare(configu.Name, ac.config[i].Name) == 0 {
			logger.LogInfo("Aggregation already exists.")
			fmt.Fprintf(w, "{\"status\" : \"success\"}")
			logger.LogInfo("Exiting handleCreateAggregation()")
			return
		}
	}

	if err := createNewAggregation(ac.createNewAggregation, configu); err != nil {
		logger.LogInfo("Error creating new aggregation: ", err.Error())
		fmt.Fprintf(w, "{\"status\" : \"failure\"}")
		logger.LogInfo("Exiting handleCreateAggregation()")
		return
	}

	ac.config = append(ac.config, configu)

	if strings.Compare("QUEUE", configu.CollectorType) == 0 {
		if ac.queueList[configu.Collectors] != 1 {
			ac.queueList[configu.Collectors] = 1
			ac.listenOnNewQueue(configu.Collectors)
			logger.LogInfo(fmt.Sprintf("Started listening on new queue %s, new aggregation has been saved successfully.", configu.Collectors))
		} else {
			logger.LogInfo("Process is already listening on this queue, new aggregation has been saved successfully.")
		}
	}

	fmt.Fprintf(w, "{\"status\" : \"success\"}")
	logger.LogInfo("Exiting handleCreateAggregation()")
}

/**
 *  initializeAndConfigureFromDB initializes database objects
 *
 *  Use prepared statements for processing because they are thread safe
 */
func (ac *AggregatorController) initializeAndConfigureFromDB() error {
	logger.LogInfo("Entering initializeAndCoinfigureFromDB()")
	var err error
	ac.aggDb, err = connectDatabase()
	if err != nil {
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.getAggregations, err = ac.aggDb.Prepare("SELECT aggregation_name FROM aggregator.aggregation")

	if err != nil {
		logger.LogInfo("Error preparing get aggregation SQL command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.aggGetConfigStmt, err = ac.aggDb.Prepare("SELECT * FROM aggregator.aggregation")
	if err != nil {
		logger.LogInfo("Error preparing get configuration SQL command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.createNewAggregation, err = ac.aggDb.Prepare("INSERT INTO aggregator.aggregation(aggregation_name, correlation_expression, completion_condition, completion_value, collectors, collector_type, destination" +
		", destination_type, aggregation_strategy) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)")
	if err != nil {
		logger.LogInfo("Error preparing create new aggregation SQL command")
		logger.LogInfo("Exiting intializeAndConfigureFromDB()")
		return err
	}

	ac.createNewAggregationInstance, err = ac.aggDb.Prepare("INSERT INTO aggregator.aggregation_instance(aggregation_name, correlation_id, aggregator_messages, completion_condition, completion_value) VALUES($1, $2, ARRAY[]::JSON[], $3, $4)")

	if err != nil {
		logger.LogInfo("Error preparing create new aggregation instance SQL command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.addMessageToAggInstance, err = ac.aggDb.Prepare("UPDATE aggregator.aggregation_instance SET aggregator_messages = ARRAY_APPEND(aggregator_messages, $1)" +
		" WHERE aggregation_name = $2 AND " +
		"correlation_id = $3")

	if err != nil {
		logger.LogInfo("Error preparing add message to aggregation instance SQL command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.checkAggInstanceExists, err = ac.aggDb.Prepare("SELECT * FROM aggregator.aggregation_instance WHERE aggregation_name = $1 AND correlation_id = $2")

	if err != nil {
		logger.LogInfo("Error preparing check if aggregation instance exists SQL command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.countNumMessages, err = ac.aggDb.Prepare("select count(*) FROM (select unnest(aggregator_messages) from aggregator.aggregation_instance where aggregation_name = $1 AND correlation_id = $2) AS AGG")

	if err != nil {
		logger.LogInfo("Error preparing count messages SQL command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.getMessages, err = ac.aggDb.Prepare("select unnest(aggregator_messages) from aggregator.aggregation_instance where aggregation_name = $1 AND correlation_id = $2")

	if err != nil {
		logger.LogInfo("Error preparing retrieve messages SQL command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.deleteMessages, err = ac.aggDb.Prepare("DELETE FROM aggregator.aggregation_instance WHERE aggregation_name = $1 AND correlation_id = $2")

	if err != nil {
		logger.LogInfo("Error preparing delete aggregation instance SQL command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.deleteAggregationStmt, err = ac.aggDb.Prepare("DELETE FROM aggregator.aggregation WHERE aggregation_name = $1")

	if err != nil {
		logger.LogInfo("Error preparing delete aggregation configuration SQL command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.DeleteAllInstancesAssociatedWithAggregation, err = ac.aggDb.Prepare("DELETE FROM aggregator.aggregation_instance WHERE aggregation_name = $1")

	if err != nil {
		logger.LogInfo("Error preparing delete aggregation configuration SQL command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.checkAggExistStmt, err = ac.aggDb.Prepare("SELECT * FROM aggregator.aggregation WHERE aggregation_name = $1")

	if err != nil {
		logger.LogInfo("Error preparing check aggregation instance exists command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.updateAggInstance, err = ac.aggDb.Prepare("UPDATE aggregator.aggregation_instance SET completion_condition = $3, completion_value = $4 WHERE correlation_id = $1 AND aggregation_name = $2")

	if err != nil {
		logger.LogInfo("Error preparing update aggregation instance command")
		logger.LogInfo("Exiting initializeAndConfigureFromDB()")
		return err
	}

	ac.retrieveCCandVal, err = ac.aggDb.Prepare("SELECT completion_condition, completion_value FROM aggregator.aggregation_instance WHERE aggregation_name = $1 AND correlation_id = $2")

	if err != nil {
		logger.LogInfo("Error preparing retrieve completion condition and values statement")
		logger.LogInfo("Exiting initializeAndConfigureFromDB")
		return err
	}

	ac.getAggregationConfig()
	ac.registerRouter()
	logger.LogInfo("Exiting initalizeAndConfigureFromDB()")
	return nil
}

/**
 *  init initializes the AggregatorController
 */
func (ac *AggregatorController) init() error {
	logger.LogInfo("Entering error()")
	if err := ac.initializeAndConfigureFromDB(); err != nil {
		logger.LogInfo("Exiting error()")
		return err
	}

	ac.queueList = make(map[string]int)
	logger.LogInfo("Exiting error()")
	return nil
}

/**
 *  listenOnNewQueue sawns new goroutine listening on queueName
 */
func (ac *AggregatorController) listenOnNewQueue(queueName string) {
	logger.LogInfo("Entering listenOnNewQueue()")
	serverAddr := fmt.Sprintf("%s:%d", *server, *port)
	var localBrok ActiveMQ_Broker
	go localBrok.connectAndListen(serverAddr, queueName)
	logger.LogInfo("Exiting listenOnNewQueue()")
}

/**
 *  StartListening spawns off goroutines to listen on each configured queue,
 *  Initiate the REST http listener process
 */
func (ac *AggregatorController) startListening() {
	logger.LogInfo("Entering startListening()")
	var i int
	ac.listenOnREST = false
	serverAddr := fmt.Sprintf("%s:%d", *server, *port)

	/* Global broker for just sending messages */
	globalActiveMQBroker.connectBroker(serverAddr)

	logger.LogInfo("Processing configuration to determine queues and endpoints to listen on")

	for i = 0; i < len(ac.config); i++ {
		if strings.Compare(ac.config[i].CollectorType, "QUEUE") == 0 {
			/* Separate broker for connecting and listening on queues */
			if ac.queueList[ac.config[i].Collectors] != 1 {
				var newBroker *ActiveMQ_Broker = new(ActiveMQ_Broker)
				logger.LogInfo("Listening on queue: ", ac.config[i].Collectors)
				/* Only need to listen on one unique queue */
				ac.queueList[ac.config[i].Collectors] = 1
				go newBroker.connectAndListen(serverAddr, ac.config[i].Collectors)
			}
		} else if strings.Compare(ac.config[i].CollectorType, "REST") == 0 {
			logger.LogInfo("Listening on REST endpoint /collect")
			ac.listenOnREST = true
		}
	}

	logger.LogInfo("Aggregator is now listening on configured endpoints")

	ac.listen()
	logger.LogInfo("Exiting startListening()")
}
