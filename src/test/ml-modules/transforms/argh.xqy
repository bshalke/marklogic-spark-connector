xquery version "1.0-ml";
module namespace example = "http://marklogic.com/rest-api/transform/argh";

declare function example:transform(
  $context as map:map,
  $params as map:map,
  $content as document-node()
) as document-node()
{
  xdmp:log($context),
  xdmp:log(("KIND", xdmp:node-kind($content/node()))),

  if (map:get($context, "input-type") = "application/xml") then
    document {
      <doc>{$content//text()[fn:normalize-space(.) != '']}</doc>
    }
  else $content
};
