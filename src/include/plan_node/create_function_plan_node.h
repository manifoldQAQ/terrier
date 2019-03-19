#pragma once

#include <string>
#include <vector>
#include "parser/create_function_statement.h"
#include "parser/parser_defs.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier {

namespace parser {
class CreateFunctionStatement;
}

namespace plan_node {
/**
 * Plan node for creating user defined functions
 */
class CreateFunctionPlanNode : public AbstractPlanNode {
 public:
  /**
   * Instantiate a new CreateFunctionPlanNode
   * @param create_func_stmt the SQL CREATE FUNCTION statement
   */
  explicit CreateFunctionPlanNode(parser::CreateFunctionStatement *create_func_stmt);

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE_FUNC; }

  /**
   * @return name of the user defined function
   */
  std::string GetFunctionName() const { return function_name; }

  /**
   * @return language type of the user defined function
   */
  parser::PLType GetUDFLanguage() const { return language; }

  /**
   * @return body of the user defined function
   */
  std::vector<std::string> GetFunctionBody() const { return function_body; }

  /**
   * @return parameter names of the user defined function
   */
  std::vector<std::string> GetFunctionParameterNames() const { return function_param_names; }

  /**
   * @return parameter types of the user defined function
   */
  std::vector<parser::Parameter::DataType> GetFunctionParameterTypes() const { return function_param_types; }

  /**
   * @return return type of the user defined function
   */
  parser::Parameter::DataType GetReturnType() const { return return_type; }

  /**
   * @return whether the definition of the user defined function needs to be replaced
   */
  bool IsReplace() const { return is_replace; }

  /**
   * @return number of parameters of the user defined function
   */
  int GetNumParams() const { return param_count; }

  DISALLOW_COPY_AND_MOVE(CreateFunctionPlanNode);

 private:
  // Indicates the UDF language type
  parser::PLType language;

  // Function parameters names passed to the UDF
  std::vector<std::string> function_param_names;

  // Function parameter types passed to the UDF
  std::vector<parser::Parameter::DataType> function_param_types;

  // Query string/ function body of the UDF
  std::vector<std::string> function_body;

  // Indicates if the function definition needs to be replaced
  bool is_replace;

  // Function name of the UDF
  std::string function_name;

  // Return type of the UDF
  parser::Parameter::DataType return_type;

  int param_count = 0;
};
}  // namespace plan_node
}  // namespace terrier
