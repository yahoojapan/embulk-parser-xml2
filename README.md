# Xml2 parser plugin for Embulk

Embulk parser plugin for parsing xml data. this plugin uses SAX parser, so you can parse very huge XML data with this plugin. also, support parsing sub-element under the root element which you specified. so you can parse and expand data more flexibly.

## Overview

* **Plugin type**: parser
* **Guess supported**: no

## Configuration

- **type**: specify this plugin as `"xml2"` (string, required)
- **root**: root element to start fetching each entries (integer, required)
- **schema**: specify the attribute of table and data type (required)

## Example

```yaml
parser:
  type: xml2
  root: mediawiki/page
  schema:
    - { name: id, type: long }
    - { name: title, type: string }
    - { name: revision/timestamp, type: timestamp, format: '%Y-%m-%dT%H:%M:%SZ' }
    - { name: revision/text, type: string }
```

Then you can fetch entries from the following xml (wikipedia archive xml format.) :
```xml
<mediawiki>
  <page>
    <id>1</id>
    <title>title 1</title>
    <revision>
      <timestamp>2004-04-30T14:46:00Z</timestamp>
      <text>body text</text>
    </revision>
  </page>
  <page>
    <id>2</id>
    <title>title 2</title>
    <revision>
      <timestamp>2004-04-30T14:46:00Z</timestamp>
      <text>body text</text>
    </revision>
  </page>
</mediawiki>
```

## Build

```
$ ./gradlew gem  
```

## How to send Pull Request 

If you would like to send a patch or Pull Request to this repository, please agree with our CLA before that. Please check following steps.

1. You send Pull Request to our Yahoo! JAPAN OSS.
2. We send you CLA to get agreement from you.
  - Yahoo! JAPAN CLA　https://gist.github.com/ydnjp/3095832f100d5c3d2592
3. You agree with the CLA.
4. We review your Pull Request and merge it. 


