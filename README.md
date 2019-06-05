# priest

This app receives
- a csv containing a table of sellers with their respective promise to deliver to a certain
region in a certain time. Each seller could have one or zero promises to deliver for each region.
- a n number of templates do be outputed and a
- m max number of templates to be investigated

A template is one chosen option of leadtime for different regions.
Regions could all have the same lead time all different leadtimes or any in between combination.
If there are 10 regions and 5 options, there would be 5^10 = 9,765,625 possible templates.

This app should return n templates (according to input) with the maximum average adherence
in the m templates investigated.

A seller delivers to r regions.
A seller adherence to a template is the amount of regions that sellers
delivers in the templates corresponding leadtime for that region devided
by the total number of regions the seller delivers to

A template average adherence is the sum of all the sellers adherence to that template
divided by the total number of sellers

## Usage

    $ java -jar priest-0.1.0-standalone.jar [input file] [number of template to be output] [number of templates to be investigated]


## Examples

...

### Bugs

...

### Any Other Sections
### That You Think
### Might be Useful

## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
