/*=========================================================================

 Name:        d3.proposalNetwork.js

 Author:      David Borland, The Renaissance Computing Institute (RENCI)

 Copyright:   The Renaissance Computing Institute (RENCI)

 Description: Proposal network visualization for Duke TIC

 =========================================================================*/

(function() {
  d3.proposalNetwork = function() {
        // Size
    var margin = { top: 5, left: 5, bottom: 5, right: 5 },
        width = 800,
        height = 800,
        innerWidth = function() { return width - margin.left - margin.right; },
        innerHeight = function() { return height - margin.top - margin.bottom; },

        // Events
        event = d3.dispatch("highlightNode"),

        // Data
        data = [],
        network = [],

        // Layout
        force = d3.forceSimulation()
            .force("link", d3.forceLink())
            .on("tick", updateForce),

        // Appearance
        radiusRange = [4, 20],

        // Scales
        radiusScale = d3.scaleSqrt(),

        // Start with empty selections
        svg = d3.select(),

        // Event dispatcher
        dispatcher = d3.dispatch("highlightNode");

    // Create a closure containing the above variables
    function proposalNetwork(selection) {
      selection.each(function(d) {
        // Save data
        data = d;

        // Process data
        processData();

        // Select the svg element, if it exists
        svg = d3.select(this).selectAll("svg")
            .data([data]);

        // Otherwise create the skeletal chart
        var svgEnter = svg.enter().append("svg")
            .attr("class", "proposalNetwork");

        g = svgEnter.append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        // Groups for layout
        var groups = ["links", "nodes"];

        g.selectAll("g")
            .data(groups)
          .enter().append("g")
            .attr("class", function(d) { return d; });

        svg = svgEnter.merge(svg);

        draw();
      });
    }

    function processData() {
      console.log(data);

      // First get all unique PIs, proposals, and orgs
      var pis = d3.map(),
          proposals = d3.map(),
          orgs = d3.map();

      data.forEach(function(d) {
        var n = name(d),
            p = d.proposal_id,
            o = d.org_name;

        addNode(pis, name(d), "pi");
        addNode(proposals, d.proposal_id, "proposal");
        addNode(orgs, d.org_name, "org");
      });

      // Initialize objects for each
      console.log(pis.values());

      // Now link
      var links = [];

      data.forEach(function(d) {
        var pi = pis.get(name(d)),
            proposal = proposals.get(d.proposal_id),
            org = orgs.get(d.org_name);

        addLink(pi, proposal);
        addLink(proposal, org);
      });

      var nodes = pis.values().concat(proposals.values()).concat(orgs.values());

      var nodeTypes = nodes.reduce(function(p, c) {
        if (p.indexOf(c.type) === -1) p.push(c.type);
        return p;
      }, []);

      network = {
        nodes: nodes,
        nodeTypes: nodeTypes,
        links: links
      };

      function name(d) {
        return d.pi_firstname + "_" + d.pi_lastname
      }

      function addNode(map, key, type) {
        if (!map.has(key)) {
          map.set(key, {
            type: type,
            name: key,
            links: []
          });
        }
      }

      function addLink(node1, node2) {
        var link = {
          source: node1,
          target: node2,
          type: node1.type + "_" + node2.type
        };

        node1.links.push(link);
        node2.links.push(link);
        links.push(link);
      }
    }

    function updateForce() {
      svg.select(".nodes").selectAll(".node")
          .attr("transform", function(d) {
            return "translate(" + d.x + "," + d.y + ")";
          });

      svg.select(".links").selectAll(".link")
          .attr("x1", function(d) { return d.source.x; })
          .attr("y1", function(d) { return d.source.y; })
          .attr("x2", function(d) { return d.target.x; })
          .attr("y2", function(d) { return d.target.y; });
    }

    function draw() {
      // Set width and height
      svg .attr("width", width)
          .attr("height", height);

      var manyBodyStrength = -0.02 / network.nodes.length * width * height;

      // Set force directed network
      force
          .force("center", d3.forceCenter(width / 2, height / 2))
          .force("manyBody", d3.forceManyBody().strength(manyBodyStrength))
          .force("collide", d3.forceCollide().radius(nodeRadius))
          .force("x", d3.forceX(width / 2))
          .force("y", d3.forceY(height / 2))
          .nodes(network.nodes)
          .force("link").links(network.links)

      force
          .alpha(1)
          .restart();

      // Draw the visualization
      drawNodes();
      drawLinks();

      function drawNodes() {
        // Create scales
        var nodeColorScale = d3.scaleOrdinal(d3.schemeCategory10)
            .domain(network.nodeTypes);

        radiusScale
            .domain([0, d3.max(network.nodes, function(d) {
              return d.links.length;
            })])
            .range(radiusRange);

        // Bind nodes
        var node = svg.select(".nodes").selectAll(".node")
            .data(network.nodes);

        // Node enter
        var nodeEnter = node.enter().append("g")
            .attr("class", "node");

        nodeEnter.append("circle")
            .style("stroke", "black");

        // Node update
        nodeEnter.merge(node).select("circle")
            .attr("r", nodeRadius)
            .style("fill", nodeFill);

        // Node exit
        node.exit().remove();

        function nodeFill(d) {
          return nodeColorScale(d.type);
        }
      }

      function drawLinks() {
        // Bind data for links
        var link = svg.select(".links").selectAll(".link")
            .data(network.links);

        // Link enter
        var linkEnter = link.enter().append("line")
            .attr("class", "link")
            .style("fill", "none")
            .style("stroke", "black");

        // Link exit
        link.exit().remove();
      }
    }

    function nodeRadius(d) {
      return radiusScale(d.links.length);
    }

    // Getters/setters

    proposalNetwork.width = function(_) {
      if (!arguments.length) return width;
      width = _;
      return proposalNetwork;
    };

    proposalNetwork.height = function(_) {
      if (!arguments.length) return height;
      height = _;
      return proposalNetwork;
    };

    // For registering event callbacks
    proposalNetwork.on = function() {
      var value = dispatcher.on.apply(dispatcher, arguments);
      return value === dispatcher ? proposalNetwork : value;
    };

    return proposalNetwork;
  };
})();
