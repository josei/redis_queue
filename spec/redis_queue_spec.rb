require 'redis_queue'

describe RedisQueue do
  let(:queue) { RedisQueue.new.tap { |q| q.clear } }

  it "pops pushed message" do
    queue.push "message"
    queue.pop.should == "message"
  end

  it "pops preserving order" do
    queue.push "message 1"
    queue.push "message 2"
    queue.pop.should == "message 1"
    queue.pop.should == "message 2"
  end

  it "pops preserving priority" do
    queue.push "message 1"
    queue.push "message 2", true
    queue.push "message 3", true
    queue.push "message 4", true
    queue.push "message 5"
    queue.pop.should == "message 2"
    queue.pop.should == "message 3"
    queue.pop.should == "message 4"
    queue.pop.should == "message 1"
    queue.pop.should == "message 5"
  end

  it "returns queue size" do
    queue.push "message 1"
    queue.push "message 2"
    queue.size.should == 2
  end

  it "returns queue list" do
    queue.push "message 1"
    queue.push "message 2"
    queue.list.should == ["message 1", "message 2"]
  end

  it "restarts queue" do
    queue.push "message 1"
    queue.done queue.pop
    queue.restart
    queue.pop.should == "message 1"
  end

  describe "in use list" do
    before do
      queue.push "message"
      queue.pop
    end

    it "returns size" do
      queue.in_use_size.should == 1
    end

    it "returns list" do
      queue.in_use_list.should == ["message"]
    end

    it "empties list when done" do
      queue.done "message"
      queue.in_use_list.should == []
    end

    it "empties list when failed" do
      queue.fail "message"
      queue.in_use_list.should == []
    end

    it "empties list when forgetting" do
      queue.forget "message"
      queue.in_use_list.should == []
    end
  end

  describe "failed list" do
    before do
      queue.push "message"
      queue.pop
      queue.fail "message"
    end

    it "returns size" do
      queue.failed_size.should == 1
    end

    it "returns list" do
      queue.failed_list.should == ["message"]
    end
  end

  describe "done list" do
    before do
      queue.push "message"
      queue.pop
      queue.done "message"
    end

    it "returns size" do
      queue.done_size.should == 1
    end

    it "returns list" do
      queue.done_list.should == ["message"]
    end
  end

  it "unpops message" do
    queue.push "message 1"
    queue.push "message 2"
    queue.unpop queue.pop
    queue.pop.should ==  "message 1"
  end

  it "repushes message" do
    queue.push "message 1"
    queue.push "message 2"
    queue.repush queue.pop
    queue.in_use_size.should == 0
    queue.pop.should ==  "message 2"
    queue.pop.should ==  "message 1"
  end

  it "resets by putting used messages back to queue" do
    queue.push "message 1"
    queue.push "message 2"
    queue.push "message 3"
    queue.pop
    queue.pop
    queue.reset
    queue.list.should ==  ["message 3", "message 1", "message 2"]
  end
end
